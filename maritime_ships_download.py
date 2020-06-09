import aiohttp
from aiohttp.client_exceptions import ClientResponseError
import asyncio
import os
import sys
import sentry_sdk
from sentry_sdk.integrations.aiohttp import AioHttpIntegration
from tqdm import tqdm
import logging
import datetime

sentry_sdk.init(
    os.environ.get('SENTRY_TOKEN'),
    integrations=[AioHttpIntegration()]
)

TOTAL_PAGE_COUNT = 8082
LIMIT = 20
URL = 'http://maritime-connector.com/ship-search/?page={0}'

formatter = logging.Formatter("%(asctime)s — %(levelname)s — %(name)s — %(message)s")
logger = logging.getLogger('ships_download')
logger.setLevel(logging.ERROR)
fh = logging.FileHandler('ships_download.log', encoding='utf-8')
fh.setFormatter(formatter)
logger.addHandler(fh)

semaphore = asyncio.Semaphore(LIMIT)

async def fetch(page, session):
    url = URL.format(page)
    filename = f'data/ships/{page}.html'
    
    if os.path.isfile(filename):
        return

    try:
        async with session.get(url, raise_for_status=True) as response:
            data = await response.read()
            with open(filename, 'w', encoding='utf-8') as file:
                file.write(data.decode('utf-8'))
    except ClientResponseError as cre:
        if cre.status == 404:
            pass
        elif cre.status == 524:
            logger.error(url)
        else:
            raise


async def bound_fetch(semaphore, page, session):
    async with semaphore:
        await fetch(page, session)


async def download_ships(pages):
    tasks = []

    async with aiohttp.ClientSession() as session:
        for page in pages:
            task = asyncio.ensure_future(bound_fetch(semaphore, page, session))
            tasks.append(task)
        
        progress_bar = tqdm(total=len(pages), desc='Download ships')

        for coro in asyncio.as_completed(tasks):
            await coro
            progress_bar.update()


if __name__ == '__main__':
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(download_ships(range(TOTAL_PAGE_COUNT, 0, -1)))
    except KeyboardInterrupt:
        pass
