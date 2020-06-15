import aiohttp
import asyncio
import os
import sys
import sentry_sdk
from sentry_sdk.integrations.aiohttp import AioHttpIntegration
from tqdm import tqdm
import peewee
from bs4 import BeautifulSoup
from bs4.element import Tag
import environ
from playhouse.db_url import connect
import timeit

env = environ.Env()
environ.Env.read_env(env_file='dev.env')
db = connect(env('DATABASE_URL'))

sentry_sdk.init(
    env('SENTRY_TOKEN'),
    integrations=[AioHttpIntegration()]
)

class BaseModel(peewee.Model):
    class Meta:
        database = db

class Department(BaseModel):
    name = peewee.CharField()

class Rank(BaseModel):
    name = peewee.CharField()

class Nationality(BaseModel):
    name = peewee.CharField()

class ShipType(BaseModel):
    name = peewee.CharField()

class Company(BaseModel):
    name = peewee.CharField()

class Vessel(BaseModel):
    name = peewee.CharField(null = True)
    href = peewee.CharField(null = True)

class Seafarer(BaseModel):
    id = peewee.IntegerField(unique=True)
    name = peewee.CharField()
    department = peewee.ForeignKeyField(Department, null=True)
    rank = peewee.ForeignKeyField(Rank, null = True)
    nationality = peewee.ForeignKeyField(Nationality, null=True)

class ServiceRecord(BaseModel):
    seafarer = peewee.ForeignKeyField(Seafarer, on_delete='CASCADE')
    department = peewee.ForeignKeyField(Department, null=True)
    rank = peewee.ForeignKeyField(Rank, null=True)
    ship_type = peewee.ForeignKeyField(ShipType, null=True)
    vessel = peewee.ForeignKeyField(Vessel, null=True)
    company = peewee.ForeignKeyField(Company, null=True)
    from_date = peewee.DateField()
    to_date = peewee.DateField()


TOTAL_PAGE_COUNT = 163552
LIMIT = 20

semaphore = asyncio.Semaphore(LIMIT)

async def fetch(url, session, filename):
    try:
        async with session.get(url, raise_for_status=True) as response:
            data = await response.read()
            return filename, data
    except aiohttp.client_exceptions.ClientResponseError as cre:
        if cre.status != 404:
            raise


async def bound_fetch(semaphore, url, session, filename):
    async with semaphore:
        return await fetch(url, session, filename)


async def download_by_ids(urlformat, fileformat, ids, pb_desc= 'Download'):
    tasks = []

    async with aiohttp.ClientSession() as session:
        for _id in ids:
            filename = fileformat.format(_id)
            if os.path.isfile(filename):
                continue
            url = urlformat.format(_id)
            task = asyncio.ensure_future(bound_fetch(semaphore, url, session, filename))
            tasks.append(task)
        
        progress_bar = tqdm(total=len(ids), desc=pb_desc)

        for coro in asyncio.as_completed(tasks):
            result = await coro
            if result is not None:
                filename, data = result                
                with open(filename, 'w', encoding='utf-8') as file:
                    file.write(data.decode('utf-8'))
                break
            progress_bar.update()


# if __name__ == '__main__':
#     try:
#         loop = asyncio.get_event_loop()
#         loop.run_until_complete(download_by_ids(
#             'http://maritime-connector.com/seafarer/a/{0}',
#             'test/{0}.html',
#             range(TOTAL_PAGE_COUNT, TOTAL_PAGE_COUNT - 1, -1),
#             'Download maritimes',
#         ))
#         sys.exit()
#     except KeyboardInterrupt:
#         pass

progress = tqdm(total=len(os.listdir('data/seafarers/')))

def parse_html(filename):
    pk = int(filename.split('.html')[0])
    page = BeautifulSoup(open(f'data/seafarers/{filename}', encoding='utf-8').read(), 'lxml')

    try:
        cv = page.find('div', {'id': 'personal-cv'})
        title = cv.find('div', {'class': 'description'}).find('h2')
        title = title.string.strip()
    except AttributeError:
        progress.update()
        return
    
    parts = page.select('h3')        
    department = ''
    rank = ''
    nationality = ''
    records = []

    for part in parts:
        if not isinstance(part, Tag):
            continue
        if not part.text:
            continue
        text = part.text.strip()
        rows = part.parent.find_next('table', {'class': 'cv-data-table'}).select('tr')

        if text == 'Personal data':
            for row in rows:
                for cell in row.children:
                    if not isinstance(cell, Tag):
                        continue
                    if not cell.string:
                        continue
                    cell_text = cell.string.strip()
                    if cell_text == 'Current department':
                        department = cell.parent.find_next('td').string.strip()
                    elif cell_text == 'Current rank':
                        rank = cell.parent.find_next('td').string.strip()
        elif text == 'Passport':                
            for row in rows:
                offset = 0
                for cell in row.children:
                    if not isinstance(cell, Tag):
                        continue
                    if not cell.string:
                        continue
                    cell_text = cell.string.strip()
                    if cell_text == 'Nationality':
                        data_cells = cell.parent.find_next('tr').select('td')
                        nationality = data_cells[offset].string.strip()
                    offset += 1
        elif text == 'Service records':
                row_index = 0
                for row in rows:
                    if row_index == 0:
                        row_index += 1
                        continue

                    record = {}                    
                    cells = row.find_all('td')
                    if (len(cells) >= 7):
                        record['department'] = cells[0].string.strip() if cells[0].string else None
                        record['rank'] = cells[1].string.strip() if cells[1].string else None
                        record['ship_type'] = cells[2].string.strip() if cells[2].string else None
                        
                        if cells[3].string:
                            record['vessel_name'] = cells[3].string.strip()
                        elif isinstance(cells[3], Tag):
                            record['vessel_href'] = cells[3].find('a').attrs['href']

                        record['company'] = cells[4].string.strip() if cells[4].string else None
                        record['from'] = cells[5].string.strip() if cells[5].string else None
                        record['to'] = cells[6].string.strip() if cells[6].string else None
                        
                    if row_index > 0 and bool(record):
                        records.append(record)
                    row_index += 1

    return {        
        'pk': pk,
        'title': title,
        'department': department,
        'rank': rank,
        'nationality': nationality,
        'service_records': records,
    }

def read_seafarers():

    files = sorted(os.listdir('data/seafarers/'), key=lambda filename: int(filename.split('.html')[0]))

    for filename in files:
        obj = parse_html(filename)
        if obj:
            yield obj

def get_or_create(model, **kwargs):
    kwargs = {k: v for k, v in kwargs.items() if v}
    if not kwargs: return None
    return model.get_or_create(**kwargs)[0]

# db.create_tables((Department, Rank, Nationality, ShipType, Company, ServiceRecord, Seafarer, Vessel))

for seafarer in read_seafarers():

    try:
        obj = Seafarer.get_by_id(seafarer['pk'])
        continue
    except peewee.DoesNotExist:
        pass

    Seafarer.insert(
        id = seafarer['pk'],
        name = seafarer['title'],
        department = get_or_create(Department, name=seafarer['department']),
        rank = get_or_create(Rank, name=seafarer['rank']),
        nationality = get_or_create(Nationality, name=seafarer['nationality'])
    ).execute()
    
    records = []
    for record in seafarer['service_records']:
        records.append({
            'seafarer_id': seafarer['pk'],
            'department': get_or_create(Department, name=record['department']),
            'rank': get_or_create(Rank, name=record['rank']),
            'ship_type': get_or_create(ShipType, name=record['ship_type']),
            'vessel': get_or_create(Vessel, name=record.get('vessel_name'), href=record.get('vessel_href')),
            'company': get_or_create(Company, name=record['company']),
            'from_date': record['from'],
            'to_date': record['to'],
        })
    ServiceRecord.insert_many(rows=records).execute()
    progress.update()
