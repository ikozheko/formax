import aiohttp
from aiohttp.client_exceptions import ClientResponseError
import asyncio
import os
import sys
import sentry_sdk
from sentry_sdk.integrations.aiohttp import AioHttpIntegration
from tqdm import tqdm

sentry_sdk.init(
    os.environ.get('SENTRY_TOKEN'),
    integrations=[AioHttpIntegration()]
)

TOTAL_PAGE_COUNT = 163552
LIMIT = 20
URL = 'http://maritime-connector.com/seafarer/a/{0}'

semaphore = asyncio.Semaphore(LIMIT)

async def fetch(page, session):
    url = URL.format(page)
    filename = f'data/seafarers/{page}.html'
    
    if os.path.isfile(filename):
        return

    try:
        async with session.get(url, raise_for_status=True) as response:
            data = await response.read()
            with open(filename, 'w', encoding='utf-8') as file:
                file.write(data.decode('utf-8'))
    except ClientResponseError as cre:
        if cre.status != 404:
            raise


async def bound_fetch(semaphore, page, session):
    async with semaphore:
        await fetch(page, session)


async def download_maritimes(pages):
    tasks = []

    async with aiohttp.ClientSession() as session:
        for page in pages:
            task = asyncio.ensure_future(bound_fetch(semaphore, page, session))
            tasks.append(task)
        
        progress_bar = tqdm(total=len(pages), desc='Download maritimes')

        for coro in asyncio.as_completed(tasks):
            await coro
            progress_bar.update()


if __name__ == '__main__':
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(download_maritimes(range(TOTAL_PAGE_COUNT, 0, -1)))
    except KeyboardInterrupt:
        pass
