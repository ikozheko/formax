import os
import sentry_sdk

sentry_sdk.init(
    'https://16fbdfb0cc614468bfd40af238171647@o404282.ingest.sentry.io/5267729',
)

TOTAL_PAGE_COUNT = 163552

downloaded_pages = set()
for filename in (os.listdir('data/seafarers')):
    page = int(filename.split('.')[0])
    downloaded_pages.add(page)

all_pages = set([page for page in range(1, TOTAL_PAGE_COUNT + 1)])

diff = all_pages.difference(downloaded_pages)
print(diff)
