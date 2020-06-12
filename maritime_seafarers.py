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
from playhouse.reflection import generate_models
from playhouse.migrate import migrate, MySQLMigrator


env = environ.Env()
environ.Env.read_env(env_file='dev.env')
db = connect(env('DATABASE_URL'))
migrator = MySQLMigrator(db)


sentry_sdk.init(
    os.environ.get('SENTRY_TOKEN'),
    integrations=[AioHttpIntegration()]
)

def db_setup():
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

    class ServiceRecord(BaseModel):
        department = peewee.ForeignKeyField(Department)
        rank = peewee.ForeignKeyField(Rank)
        ship_type = peewee.ForeignKeyField(ShipType)
        vessel = peewee.ForeignKeyField(Vessel)
        company = peewee.ForeignKeyField(Company)
        from_date = peewee.DateField()
        to_date = peewee.DateField()

    class Seafarer(BaseModel):
        id = peewee.IntegerField(unique=True)
        name = peewee.CharField()
        department = peewee.ForeignKeyField(Department)
        rank = peewee.ForeignKeyField(Rank)
        nationality = peewee.ForeignKeyField(Nationality)

    class ServiceRecords(BaseModel):
        service_record = peewee.ForeignKeyField(ServiceRecord)
        seafarer = peewee.ForeignKeyField(Seafarer)

    db.create_tables((Department, Rank, Nationality, ShipType, Company, ServiceRecord, Seafarer, ServiceRecords, Vessel))

models = generate_models(db)

if not bool(models):
    db_setup()
    models = generate_models(db)
globals().update(models)

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

def read_seafarers():
    i = 1
    files = os.listdir('data/seafarers/')
    for filename in ['14452.html']:
        pk = int(filename.split('.html')[0])
        page = BeautifulSoup(open(f'data/seafarers/{filename}').read(), 'lxml')

        cv = page.find('div', {'id': 'personal-cv'})
        title = cv.find('div', {'class': 'description'}).find('h2')
        title = title.string.strip()
        
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
                            record['department'] = cells[0].string.strip() if cells[0].string else ''
                            record['rank'] = cells[1].string.strip() if cells[1].string else ''
                            record['ship_type'] = cells[2].string.strip() if cells[2].string else ''
                            
                            if cells[3].string:
                                record['vessel_name'] = cells[3].string.strip()
                            elif isinstance(cells[3], Tag):
                                record['vessel_href'] = cells[3].find('a').attrs['href']

                            record['company'] = cells[4].string.strip() if cells[4].string else ''
                            record['from'] = cells[5].string.strip() if cells[5].string else ''
                            record['to'] = cells[6].string.strip() if cells[6].string else ''
                            
                        if row_index > 0 and bool(record):
                            records.append(record)
                        row_index += 1

        yield {        
            'pk': pk,
            'title': title,
            'department': department,
            'rank': rank,
            'nationality': nationality,
            'service_records': records,
        }


if __name__ == '__main__':
    
    for seafarer_item in read_seafarers():
        # print(len(seafarer['service_records']))
        # print(seafarer['service_records'][19]['vessel_href'])
        # print(seafarer['service_records'][19].get('vessel_name', 'None'))

        department, was_created = department.get_or_create(name = seafarer_item['department'])
        rank, was_created = rank.get_or_create(name = seafarer_item['rank'])
        nationality, was_created = nationality.get_or_create(name = seafarer_item['nationality'])
        
        try:
            seafarer_pk = seafarer.insert(
                id = int(seafarer_item['pk']),
                name = seafarer_item['title'],
                department = department,
                rank = rank,
                nationality = nationality
            ).execute()
        except peewee.IntegrityError:
            seafarer_pk = None

        for service_record in seafarer_item['service_records']:
            record = servicerecord()
            record.department, was_created = department.get_or_create(name = service_record['department'])
            record.rank, was_created = rank.get_or_create(name = service_record['rank'])
            record.ship_type, was_created = shiptype.get_or_create(name = service_record['ship_type'])
            
            try:
                vessel_pk = vessel.insert(
                    name = service_record['vessel_name'] if 'vessel_name' in service_record else None,
                    href = service_record['href'] if 'vessel_href' in service_record else None,
                ).execute()
            except peewee.IntegrityError:
                vessel_pk = None

            record.vessel = vessel.get(id=vessel_pk)

            record.company, was_created = company.get_or_create(name = service_record['company'])
            record.from_date = service_record['from']
            record.to_date = service_record['to']
            try:
                record_pk = record.save()
                print(record_pk)
                break
            except peewee.IntegrityError:
                record_pk = None
