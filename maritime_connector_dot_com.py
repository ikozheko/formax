from bs4 import BeautifulSoup
from bs4.element import Tag
import requests
import json
from console_progressbar import ProgressBar
import shutil
import os
from urllib.parse import urlparse

site = 'http://maritime-connector.com'

def get_page(page_number):
    users = []
    url = f'{site}/seafarers/?page={page_number}'
    
    response = requests.get(url)
    assert response.status_code == requests.codes.ok

    page = response.content.decode('utf-8')
    page = BeautifulSoup(page, 'html.parser')

    container = page.find('ul', {'id': 'results-list'})
    pagination = page.find('p', {'class': 'pagination'})
    next_page = pagination.find('a', {'class': 'next'}).attrs['href'] if pagination else None
    print(f'next page = {next_page}')
    items = container.select('li')
    
    for item in items:
        href = ''
        for cell in item.children:
            if not isinstance(cell, Tag):
                continue
            if cell.name == 'a':
                href = cell.attrs['href']
                break
        response = requests.get(href)
        assert response.status_code == requests.codes.ok

        page = response.content.decode('utf-8')
        page = BeautifulSoup(page, 'html.parser')
        description = page.find('div', {'class': 'description'})
        avatar = description.find('a', {'rel': 'prettyPhoto[profile]'})
        avatar_image = requests.get(avatar.attrs['href'], stream=True)

        filename = os.path.basename(urlparse(avatar.attrs['href']).path)
        with open(f'avatars/{filename}', 'wb') as file:
            avatar_image.raw.decode_content = True
            shutil.copyfileobj(avatar_image.raw, file)

        parts = page.select('h3')        
        department = ''
        rank = ''
        nationality = ''
        service_records = []
        
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

                    service_record = {}                    
                    cells = row.find_all('td')
                    service_record['department'] = cells[0].string.strip() if cells[0].string else ''
                    service_record['rank'] = cells[1].string.strip() if cells[1].string else ''
                    service_record['ship_type'] = cells[2].string.strip() if cells[2].string else ''
                    
                    if cells[3].string:
                        service_record['vessel_name'] = cells[3].string.strip()
                    else:
                        service_record['vessel_name'] = str(type(cells[3]))
                    service_record['company'] = cells[4].string.strip() if cells[4].string else ''
                    service_record['from'] = cells[5].string.strip() if cells[5].string else ''
                    service_record['to'] = cells[6].string.strip() if cells[6].string else ''
                        
                    if row_index > 0:
                        service_records.append(service_record)
                    row_index += 1
        
        users.append({
            'href': href,
            'avatar_href': avatar.attrs['href'],
            'avatar_filename': filename,
            'title': description.find('h2').string.strip(),
            'department': department,
            'rank': rank,
            'nationality': nationality,
            'service_records': service_records,
        })
    
    return users

def main():
    users = get_page(1)
    
    with open('seafarers.json', 'w') as file:
        json.dump(users, file, indent=4)

if __name__ == "__main__":
    main()
