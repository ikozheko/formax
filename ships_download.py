import aiohttp
import asyncio
import os
import sys
import sentry_sdk
from tqdm import tqdm
from environs import Env
from playhouse.db_url import connect
from peewee import Model, IntegerField, CharField, ForeignKeyField
from bs4 import BeautifulSoup
from bs4.element import Tag
import hashlib

env = Env()
env.read_env('dev.env')

sentry_sdk.init(
    os.environ.get('SENTRY_TOKEN'),
    integrations=[sentry_sdk.integrations.aiohttp.AioHttpIntegration()]
)

def url_generator():
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
    try:
        async with session.get(url, raise_for_status=True) as response:
            return await response.read()            
    except aiohttp.client_exceptions.ClientResponseError as cre:
        sentry_sdk.capture_exception(cre)

def get_filename_for_write(url):
    filehash = hashlib.md5(url.encode('utf-8')).hexdigest()
    return os.path.join(env.path('SHIP_DATA_DIR2'), filehash)

def url_is_fetched(url):
    filename = get_filename_for_write(url)
    if os.path.isfile(filename):
        return True
    return False

async def producer(q: asyncio.Queue):
    for url in tqdm(url_generator(), desc='producer'):
        await q.put(url)

async def consumer(q: asyncio.Queue, name):
    progress = tqdm(desc=f'consumer #{name}', leave=False)
    while True:
        url = await q.get()
        if url_is_fetched(url):
            q.task_done()
            continue
        async with aiohttp.ClientSession() as session:
            data = await fetch(url, session)
            with open(get_filename_for_write(url), 'w', encoding='utf-8') as file:
                file.write(data.decode('utf-8'))
        progress.update()
        q.task_done()

async def main():
    try:
        q = asyncio.Queue(maxsize=40)
        
        producer_task = asyncio.create_task(producer(q))
        consumers = [asyncio.create_task(consumer(q, name)) for name in range(20)]
        
        await producer_task
        await q.join()
        for consumer_task in consumers:
            consumer_task.cancel()

        print('proccess finished')
    except KeyboardInterrupt:
        print('proccess interrupted')

if __name__ == '__main__':
    asyncio.run(main())