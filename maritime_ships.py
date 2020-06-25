import aiohttp
import asyncio
import os
import sys
import sentry_sdk
from tqdm import tqdm
from environs import Env
from bs4 import BeautifulSoup
from bs4.element import Tag
import hashlib
import peewee
from peewee import Model, IntegerField, CharField, ForeignKeyField
from playhouse.db_url import connect
from playhouse.shortcuts import model_to_dict, dict_to_model
from playhouse.migrate import migrate, MySQLMigrator

env = Env()
env.read_env('dev.env')

sentry_sdk.init(
    os.environ.get('SENTRY_TOKEN'),
    integrations=[sentry_sdk.integrations.aiohttp.AioHttpIntegration()]
)

def url_generator():
    """ Генератор ссылок на корабли
    
    Проходит по всем страницам кораблей и из каждой страницы извлекает
    все ссылки на корабли.
    """
    for entry in os.scandir(env.path('SHIP_PAGE_DATA_DIR')):
        if not entry.name.endswith('.html'): continue
        html = open(entry.path, 'r', encoding='utf-8').read()                
        page = BeautifulSoup(html, 'lxml')
        items = page.find('ul', {'id': 'results-list'}).find_all('li')
        urls = []
        for item in items:
            if not isinstance(item, Tag):
                continue
            urls.append(item.find('a').attrs['href'])
        yield from urls

async def fetch(url, session: aiohttp.ClientSession):
    """ Функция для скачивания файла по его URL.

    В случае возникновения ошибок логгирует их в SENTRY.
    """
    try:
        async with session.get(url, raise_for_status=True) as response:
            return await response.read()            
    except aiohttp.client_exceptions.ClientResponseError as cre:
        sentry_sdk.capture_exception(cre)

def get_filename_for_write(url):
    """ Функция для генерации имени файла по его URL.

    Используется для сохранения скачанных файлов, а также
    для предотвращения повторного скачивания.
    """
    filehash = hashlib.md5(url.encode('utf-8')).hexdigest()
    return os.path.join(env.path('SHIP_DATA_DIR'), filehash)

def url_is_fetched(url):
    """ Функция для проверки, скачан файл или нет.
    """
    filename = get_filename_for_write(url)
    if os.path.isfile(filename):
        return True
    return False

async def producer(q: asyncio.Queue):
    """ Реализация Producer """
    for url in tqdm(url_generator(), desc='producer'):
        await q.put(url)

async def consumer(q: asyncio.Queue, name):
    """ Реализация Consumer """
    progress = tqdm(desc=f'consumer #{name}', leave=False)
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36'
    }
    async with aiohttp.ClientSession(headers=headers) as session:
        while True:
            url = await q.get()
            if url_is_fetched(url):
                q.task_done()
                continue
                data = await fetch(url, session)
                with open(get_filename_for_write(url), 'w', encoding='utf-8') as file:
                    file.write(data.decode('utf-8'))
            progress.update()
            q.task_done()

def get_part_by_name(page: BeautifulSoup, part_key: str):
    """ Получение таблицы с данными по имени раздела """
    parts = page.select('h3')
    key_to_name = {
        'ship_info': 'Ship info',
    }
    if part_key not in key_to_name.keys():
        raise ValueError(f'Unknown part key "{part_key}"')

    parts = [part for part in parts if isinstance(part, Tag) and part.text.strip().startswith(key_to_name[part_key])]
    if not len(parts):
        return None
    rows = parts[0].parent.find_next('table', {'class': 'ship-data-table'}).select('tr')
    return rows

def parse_info(page: BeautifulSoup) -> dict:
    """ Парсинг страницы """
    rows = get_part_by_name(page, 'ship_info')
    if rows is None: return {}
    name_to_key = {
        'IMO number': {'key': 'imo_number', 'fn': int},
        'Name of the ship': 'name',
        'Type of ship': 'ship_type',
        'Gross tonnage': 'gross_tonnage',
        'DWT': 'dwt',
        'Manager': 'manager',
        'Owner': 'owner',
        'Manager & owner': 'managerowner',
    }
    obj = {}
    for row in rows:
        for cell in row.children:
            if not isinstance(cell, Tag):
                continue
            if not cell.string:
                continue
            cell_text = cell.string.strip()
            
            if cell_text in name_to_key.keys():
                try:
                    value = cell.parent.find_next('td').string.strip()
                    if 'key' in name_to_key[cell_text]:
                        key = name_to_key[cell_text]['key']
                        fn = name_to_key[cell_text]['fn']
                    else:
                        key = name_to_key[cell_text]
                        fn = None
                        
                    obj[key] = fn(value) if fn else value
                except AttributeError:
                    return {}
    return obj

def ship_generator():
    """ Проход по всем имеющимся файлам с кораблями, парсинг, генерация, удаление 'пустых' файлов """
    for entry in os.scandir(env.path('SHIP_DATA_DIR')):
        html = open(entry.path, 'r', encoding='utf-8').read()
        page = BeautifulSoup(html, 'lxml')
        info = parse_info(page)
        if not bool(info):
            os.remove(entry.path)
            continue
        yield info

db = connect(env('DATABASE_URL'))

class BaseModel(Model):
    class Meta:
        database = db

class ShipType(BaseModel):
    name = CharField()
    
    def __str__(self):
        return self.name

class Manager(BaseModel):
    name = CharField()

    def __str__(self):
        return self.name
    
class Owner(BaseModel):
    name = CharField()

    def __str__(self):
        return self.name

class ManagerOwner(BaseModel):
    name = CharField()

    def __str__(self):
        return self.name

class Vessel(BaseModel):
    name = CharField(null = True)
    href = CharField(null = True)
    imo_number = IntegerField()
    ship_type = ForeignKeyField(ShipType, null=True)
    gross_tonnage = CharField(null=True)
    dwt = CharField(null=True)
    manager = ForeignKeyField(Manager, null=True)
    owner = ForeignKeyField(Owner, null=True)
    managerowner = ForeignKeyField(ManagerOwner, null=True)

    def __str__(self):
        return self.name if self.name else self.href

def get_or_create(model, **kwargs):
    kwargs = {k: v for k, v in kwargs.items() if v}
    if not kwargs: return None
    return model.get_or_create(**kwargs)[0]

async def main():
    try:
        """ Асинхронное скачивание кораблей """
        q = asyncio.Queue(maxsize=40)
        
        producer_task = asyncio.create_task(producer(q))
        consumers = [asyncio.create_task(consumer(q, name)) for name in range(20)]
        
        await producer_task
        await q.join()
        for consumer_task in consumers:
            consumer_task.cancel()

        # migrator = MySQLMigrator(db)
        # db.create_tables([ManagerOwner])
        # manager_owner_field = ForeignKeyField(model=Manager, to_field=ManagerOwner.id, null=True)
        # owner_field = ForeignKeyField(model=Owner, to_field=Owner.id, null=True)
        # migrate(
        #     migrator.add_column('vessel', 'managerowner_id', manager_owner_field),
        #     migrator.rename_column('vessel', 'owner', 'owner_id'),
        # ) 
        
        """ Обновление в БД информации по кораблям """
        total = len(os.listdir(env.path('SHIP_DATA_DIR')))
        for ship in tqdm(ship_generator(), total=total):
            v = get_or_create(Vessel, imo_number=ship['imo_number'])
            v.name = ship['name'] if ship.get('name') else None
            v.ship_type = get_or_create(ShipType, name=ship['ship_type']) if ship.get('ship_type') else None
            v.gross_tonnage = ship['gross_tonnage'] if ship.get('gross_tonnage') else None
            v.dwt = ship['dwt'] if ship.get('dwt') else None
            v.manager = get_or_create(Manager, name=ship['manager']) if ship.get('manager') else None
            v.owner = get_or_create(Owner, name=ship['owner']) if ship.get('owner') else None
            v.managerowner = get_or_create(ManagerOwner, name=ship['managerowner']) if ship.get('managerowner') else None
            v.save()
            
        print('proccess finished')
    except KeyboardInterrupt:
        print('proccess interrupted')

if __name__ == '__main__':
    asyncio.run(main())