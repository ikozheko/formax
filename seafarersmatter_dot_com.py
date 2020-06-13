from bs4 import BeautifulSoup
from bs4.element import Tag
import requests
import json
from console_progressbar import ProgressBar
import sentry_sdk
import os

sentry_sdk.init(
    os.environ.get('SENTRY_TOKEN'),
)

site = 'https://seafarersmatter.com/index.php/the-letter/?'

def get_page(page_number):
    users = []
    url = f'{site}listpage={page_number}&instance=2'
    
    response = requests.get(url)
    response.raise_for_status()

    page = response.content.decode('utf-8')
    page = BeautifulSoup(page, 'html.parser')

    container = page.find('div', {'class': 'participants-database', 'id': 'participants-list-2'})
    table = container.find('table', {'class': 'list-container'})
    last_page = None
    a = container.select('li.lastpage a')
    if (len(a) > 0):
        last_page = int(a[0]['data-page'])
    
    rows = table.select('tbody tr')
    
    for row in rows:
        first_name = ''
        last_name = ''
        for cell in row.children:
            if not isinstance(cell, Tag):
                continue
            if 'first_name-field' in cell.attrs['class']:
                first_name = cell.string.strip()
            if 'last_name-field' in cell.attrs['class']:
                last_name = cell.string.strip()
        users.append({'first_name': first_name, 'last_name': last_name})
    
    return users, last_page

def main():
    all_users = []
    pb = ProgressBar(total=100,prefix='Here', suffix='Now', decimals=3, length=50, fill='\u25A0', zfill='-')
    pb.print_progress_bar(0)

    i = 1
    last_page = 1

    while last_page:
        users, last_page = get_page(i)
        all_users.extend(users)
        if last_page:
            pb.print_progress_bar((i / last_page) * 100)
        else:
            pb.print_progress_bar(100)
        i += 1

    with open('users.json', 'w') as file:
        json.dump(all_users, file, indent=4)

if __name__ == "__main__":
    main()
