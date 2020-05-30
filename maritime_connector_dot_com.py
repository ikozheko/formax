from bs4 import BeautifulSoup
from bs4.element import Tag
import requests
import json
from console_progressbar import ProgressBar
import sys
import time
import shutil
import os
from urllib.parse import urlparse

site = 'http://maritime-connector.com'

def get_total_count():
    try:
        url = f'{site}/seafarers/?page=1'
        response = requests.get(url)
        response.raise_for_status()
        page = response.content.decode('utf-8')
        page = BeautifulSoup(page, 'lxml')
        total_count = int(page.find('p', {'class': 'result-count'}).string.lower().split(' of ')[1])
    except:
        total_count = None

    return total_count


def get_page(page_number):
    users = []
    url = f'{site}/seafarers/?page={page_number}'
    
    response = requests.get(url)
    response.raise_for_status()

    page = response.content.decode('utf-8')
    page = BeautifulSoup(page, 'lxml')

    container = page.find('ul', {'id': 'results-list'})
    pagination = page.find('p', {'class': 'pagination'})

    try:
        total_count = int(page.find('p', {'class': 'result-count'}).string.lower().split(' of ')[1])
    except:
        total_count = None
    
    items = container.select('li')
    
    for item in items:
        href = ''
        for cell in item.children:
            if not isinstance(cell, Tag):
                continue
            if cell.name == 'a':
                href = cell.attrs['href']
                break        
        try:
            response = requests.get(href)
            response.raise_for_status()
        except requests.HTTPError as e:
            users.append({
                'href': href,
                'error': f'{e.response.status_code} {e.response.reason}'
            })
            continue        

        page = response.content.decode('utf-8')
        page = BeautifulSoup(page, 'lxml')
        description = page.find('div', {'class': 'description'})
        if not description:
            description = page.find('p', {'class': 'description'})
        
        avatar = description.find('a', {'rel': 'prettyPhoto[profile]'})
        if avatar:
            avatar_image = requests.get(avatar.attrs['href'], stream=True)

            filename = os.path.basename(urlparse(avatar.attrs['href']).path)
            with open(f'avatars/{filename}', 'wb') as file:
                avatar_image.raw.decode_content = True
                shutil.copyfileobj(avatar_image.raw, file)

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
            rows = part.parent.findNext('table', {'class': 'cv-data-table'}).select('tr')

            if text == 'Personal data':
                for row in rows:
                    for cell in row.children:
                        if not isinstance(cell, Tag):
                            continue
                        if not cell.string:
                            continue
                        cell_text = cell.string.strip()
                        if cell_text == 'Current department':
                            department = cell.parent.findNext('td').string.strip()
                        elif cell_text == 'Current rank':
                            rank = cell.parent.findNext('td').string.strip()
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
                            data_cells = cell.parent.findNext('tr').select('td')
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
                        record['department'] = cells[0].string.strip() if cells[0].string else ''
                        record['rank'] = cells[1].string.strip() if cells[1].string else ''
                        record['ship_type'] = cells[2].string.strip() if cells[2].string else ''
                        
                        if cells[3].string:
                            record['vessel_name'] = cells[3].string.strip()
                        elif isinstance(cells[3], Tag):
                            record['vessel_name'] = cells[3].find('a').attrs['href']

                        record['company'] = cells[4].string.strip() if cells[4].string else ''
                        record['from'] = cells[5].string.strip() if cells[5].string else ''
                        record['to'] = cells[6].string.strip() if cells[6].string else ''
                        
                    if row_index > 0 and bool(record):
                        records.append(record)
                    row_index += 1
        title = description.find('h2').string
        title = title.strip() if title else ''

        users.append({
            'href': href,
            'avatar': {
                'href': avatar.attrs['href'],
                'filename': filename,
            },
            'title': title,
            'department': department,
            'rank': rank,
            'nationality': nationality,
            'service_records': records,
        })
    
    return users, total_count


def main():
    all_users = []    

    completed_count = 0
    page = 1
    try:
        with open('seafarers.json', 'r') as file:
            data = json.load(file)
            page = int(data['completed_page']) + 1
            total_count = int(data['total_count'])
            all_users = data['data']
    except FileNotFoundError:
        total_count = get_total_count()
    
    pb = ProgressBar(total=total_count,prefix='Here', suffix='Now', decimals=3, length=50, fill='\u25A0', zfill='-')
    pb.print_progress_bar(len(all_users))
    
    while len(all_users) < total_count:
        users, total_count = get_page(page)
        all_users.extend(users)
    
        with open('seafarers.json', 'w') as file:
            json.dump({
                'completed_count': len(all_users),
                'completed_page': page,
                'total_count': total_count if total_count else page,
                'data': all_users,
            }, file, indent=4)

        pb.print_progress_bar(len(all_users))        
        page += 1
    

if __name__ == '__main__':
    main()
