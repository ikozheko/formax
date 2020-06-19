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
from bs4.element import Tag
import hashlib

env = Env()
env.read_env('dev.env')

sentry_sdk.init(
    os.environ.get('SENTRY_TOKEN'),
    integrations=[AioHttpIntegration()]
)

def iterate_pages():
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
        yield urls

async def fetch(url, session):
    try:
        async with session.get(url, raise_for_status=True) as response:
            data = await response.read()
            return url, data
    except aiohttp.client_exceptions.ClientResponseError as cre:
        sentry_sdk.capture_exception(cre)

async def bound_fetch(semaphore, url, session):
    async with semaphore:
        return await fetch(url, session)

def get_filename_for_save(url):
    filehash = hashlib.md5(url.encode('utf-8')).hexdigest()
    return os.path.join(env.path('SHIP_DATA_DIR2'), filehash)

def url_is_fetched(url):
    filename = get_filename_for_save(url)
    if os.path.isfile(filename):
        return True
    return False

async def producer(q: asyncio.Queue):
    semaphore = asyncio.Semaphore(10)
    async with aiohttp.ClientSession() as session:
        page = 1
        for urls in iterate_pages():
            progress = tqdm(total=len(urls), leave=False, desc=f'fetch page #{page}')
            tasks = [asyncio.create_task(bound_fetch(semaphore, url, session)) for url in urls if not url_is_fetched(url)]

            for coro in asyncio.as_completed(tasks):
                future = await coro
                if future is not None:
                    progress.update()
                    url, data = future
                    await q.put((url, data))
            
            page += 1

            break

async def consumer(q: asyncio.Queue):
    while True:
        url, data = await q.get()
        with open(get_filename_for_save(url), 'w', encoding='utf-8') as file:
            file.write(data.decode('utf-8'))
        q.task_done()

async def main():
    try:
        q = asyncio.Queue()
        
        producer_task = asyncio.create_task(producer(q))
        consumer_task = asyncio.create_task(consumer(q))
        
        await producer_task
        await q.join()
        consumer_task.cancel()

        print('proccess finished')
    except KeyboardInterrupt:
        print('proccess interrupted')

if __name__ == '__main__':
    asyncio.run(main())