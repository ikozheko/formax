import aiohttp
from aiohttp.client_exceptions import ClientResponseError
import asyncio
import os
import sys
import sentry_sdk
from sentry_sdk.integrations.aiohttp import AioHttpIntegration
from tqdm import tqdm
from environs import Env
from playhouse.db_url import connect
from peewee import Model, IntegerField, CharField, ForeignKeyField
from bs4 import BeautifulSoup
from bs4.element import NavigableString, Tag
import hashlib

env = Env()
env.read_env('dev.env')

# sentry_sdk.init(
#     os.environ.get('SENTRY_TOKEN'),
#     integrations=[AioHttpIntegration()]
# )

class BaseModel(Model):
    class Meta:
        database = connect(env('DATABASE_URL'))

class ShipType(BaseModel):
    name = CharField()

    def __str__(self):
        return self.name

class ManagerOwner(BaseModel):
    name = CharField()

    def __str__(self):
        return self.name

class Ship(BaseModel):
    imo_number = IntegerField()
    name = CharField()
    ship_type = ForeignKeyField(ShipType)
    gross_tonnage = CharField()
    dwt = CharField()
    manager_and_owner = ForeignKeyField(ManagerOwner)

    def __str__(self):
        return str(self.imo_number)


def read_ship_pages():
    for entry in os.scandir(env.path('SHIP_PAGE_DATA_DIR')):
        if not entry.name.endswith('.html'): continue
        page_id = int(entry.name.split('.html')[0])
        
        html = open(entry.path, 'r', encoding='utf-8').read()                
        page = BeautifulSoup(html, 'lxml')
        hrefs = page.find('ul', {'id': 'results-list'}).find_all('li')
        urls = []
        for href in hrefs:
            if not isinstance(href, Tag):
                continue
            urls.append(href.find('a').attrs['href'])
        yield page_id, urls

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

def get_filename_by_url(url):
    filehash = hashlib.md5(url.encode('utf-8')).hexdigest()                
    filename = os.path.join(env.path('SHIP_DATA_DIR'), f'{filehash}.html')
    return filename

async def download_ships():
    semaphore = asyncio.Semaphore(20)
    async with aiohttp.ClientSession() as session:
        for page_id, urls in tqdm(iterable=read_ship_pages(), total=8082):
            tasks = []
            for url in urls:
                filename = get_filename_by_url(url)
                if os.path.isfile(filename):
                    continue
                task = asyncio.ensure_future(bound_fetch(semaphore, url, session, filename))
                tasks.append(task)

            for coro in asyncio.as_completed(tasks):
                future = await coro
                if future is not None:
                    filename, data = future
                    with open(filename, 'w', encoding='utf-8') as file:
                        file.write(data.decode('utf-8'))

if __name__ == '__main__':
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(download_ships())        
        print('proccess finished')
    except KeyboardInterrupt:
        print('proccess interrupted')
