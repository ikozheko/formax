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
from peewee import IntegerField, CharField, ForeignKeyField
from playhouse.db_url import connect
from playhouse.shortcuts import model_to_dict, dict_to_model
from environs import Env

env = Env()
env.read_env('dev.env')
db = connect(env('DATABASE_URL'))
data_dir = env.path('SEAFARER_DATA_DIR')

sentry_sdk.init(
    env('SENTRY_TOKEN'),
    integrations=[AioHttpIntegration()]
)

class BaseModel(Model):
    DATE_FORMAT = '%d.%m.%Y'
    class Meta:
        database = db

class Department(BaseModel):
    name = CharField()

    def __str__(self):
        return self.name

class Rank(BaseModel):
    name = CharField()

    def __str__(self):
        return self.name

class Nationality(BaseModel):
    name = CharField()

    def __str__(self):
        return self.name

class ShipType(BaseModel):
    name = CharField()

    def __str__(self):
        return self.name

class Company(BaseModel):
    name = CharField()

    def __str__(self):
        return self.name

class ManagerOwner(BaseModel):
    name = CharField()

    def __str__(self):
        return self.name

class Vessel(BaseModel):
    name = CharField(null = True)
    href = CharField(null = True)
    imo_number = IntegerField()
    ship_type = ForeignKeyField(ShipType, null=True)
    gross_tonnage = CharField(null=True)
    dwt = CharField(null=True)
    managerowner = ForeignKeyField(ManagerOwner, null=True)

    def __str__(self):
        return self.name if self.name else self.href

class Seafarer(BaseModel):
    id = IntegerField(unique=True)
    name = CharField()
    department = ForeignKeyField(Department, null=True)
    rank = ForeignKeyField(Rank, null = True)
    nationality = ForeignKeyField(Nationality, null=True)

    def __str__(self):
        return self.name

class ServiceRecord(BaseModel):
    seafarer = ForeignKeyField(Seafarer, on_delete='CASCADE')
    department = ForeignKeyField(Department, null=True)
    rank = ForeignKeyField(Rank, null=True)
    ship_type = ForeignKeyField(ShipType, null=True)
    vessel = ForeignKeyField(Vessel, null=True)
    company = ForeignKeyField(Company, null=True)
    from_date = DateField(formats=[BaseModel.DATE_FORMAT], null=True)
    to_date = DateField(formats=[BaseModel.DATE_FORMAT], null=True)

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

def parse_personal_data(page):
    rows = get_part_by_name(page, 'personal_data')
    if rows is None: return {}
    name_to_key = {
        'Current department': 'department',
        'Current rank': 'rank',
        # 'Current ship type': 'ship_type',
        # 'Desired annual salary (USD)': 'salary',
        # 'Desired contract type': 'contract_type',
        # 'Desired contract dynamics': 'contract_dynamics',
        # 'Years of experience': 'years_of_experince',
        # 'Date available for job': 'date_available_for_job',
        # 'Marital status': 'marital_status',
    }
    obj = {}
    for row in rows:
        for cell in row.children:
            if not isinstance(cell, Tag):
                continue
            if not cell.string:
                continue
            cell_text = cell.string.strip()
            
            if cell_text in name_to_key.keys():
                obj[name_to_key[cell_text]] = cell.parent.find_next('td').string.strip()
    return obj

def parse_passport_data(page):
    rows = get_part_by_name(page, 'passport')
    if rows is None: return {}
    name_to_key = {
        'Nationality': 'nationality',
    }
    obj = {}
    for row in rows:
        offset = 0
        for cell in row.children:
            if not isinstance(cell, Tag):
                continue
            if not cell.string:
                continue
            cell_text = cell.string.strip()
            if cell_text in name_to_key.keys():
                data_cells = cell.parent.find_next('tr').select('td')
                obj[name_to_key[cell_text]] = data_cells[offset].string.strip()
            offset += 1
    return obj

def parse_service_records(page):
    rows = get_part_by_name(page, 'service_records')
    if rows is None : return []
    name_to_key = {
        'Department': 'department',
        'Rank': 'rank',
        'Ship type': 'ship_type',
        'Vessel name': 'vessel_name',
        'Company': 'company',
        'From': 'from',
        'To': 'to',
    }
    headers = [name_to_key[row.text] for row in rows[0].select('th') if isinstance(row, Tag) and row.text]
    obj = []
    for row in rows[1:]:
        cells = row.find_all('td')       
        record = [cell.string.strip() if cell.string else cell.find('a').attrs['href'] if cell.find('a') else None for cell in cells]
        obj.append(dict(zip(headers, record)))
    return obj

def get_part_by_name(page, part_key):
    parts = page.select('h3')
    key_to_name = {
        'personal_data': 'Personal data',
        'passport': 'Passport',
        'service_records': 'Service records',
    }
    if part_key not in key_to_name.keys():
        raise ValueError(f'Unknown part key "{part_key}"')

    parts = [part for part in parts if isinstance(part, Tag) and part.text.strip() == key_to_name[part_key]]
    if not len(parts):
        return None
    rows = parts[0].parent.find_next('table', {'class': 'cv-data-table'}).select('tr')
    return rows

def parse_html(html):
    page = BeautifulSoup(html, 'lxml')
    try:
        cv = page.find('div', {'id': 'personal-cv'})
        title = cv.find('div', {'class': 'description'}).find('h2').string.strip()
    except AttributeError:
        return None

    personal_data = parse_personal_data(page)
    passport_data = parse_passport_data(page)

    return {
        'title': title,
        'department': personal_data.get('department', None),
        'rank': personal_data.get('rank', None),
        'nationality': passport_data.get('nationality', None),
        'service_records': parse_service_records(page),
    }

def parse_seafarers():
    files = sorted(os.listdir(data_dir), key=lambda filename: int(filename.split('.html')[0]))
    
    for filename in tqdm(files, desc='Parsing seafarers'):
        obj_id = int(filename.split('.html')[0])
        try:
            obj = Seafarer.get_by_id(obj_id)            
            continue        
        except Seafarer.DoesNotExist:
            full_filename = f'{data_dir}/{filename}'            
            html = open(full_filename, encoding='utf-8').read()
            obj = parse_html(html)
            if obj:
                obj['id'] = obj_id
                yield obj
            else:
                os.remove(full_filename)

def test_seafarers():
    for obj in tqdm(Seafarer.select(Seafarer.id), desc='Testing seafarers'):
        if not os.path.isfile(os.path.join(data_dir, f'{obj.id}.html')):
            yield {'error': f'no html file for id #{obj.id}'}

def service_records():
    files = sorted(os.listdir(data_dir), key=lambda filename: int(filename.split('.html')[0]))
    
    for filename in tqdm(files):
        obj_id = filename.split('.html')[0]
        full_filename = os.path.join(data_dir, f'{obj_id}.html')
        html = open(full_filename, encoding='utf-8').read()
        records = parse_service_records(BeautifulSoup(html, 'lxml'))
        for record in records:
            yield obj_id, record


def get_or_create(model, **kwargs):
    kwargs = {k: v for k, v in kwargs.items() if v}
    if not kwargs: return None
    return model.get_or_create(**kwargs)[0]

def main():
    db.create_tables((Department, Rank, Nationality, ShipType, Company, ServiceRecord, Seafarer, Vessel))

    for seafarer in parse_seafarers():

        Seafarer.insert(
            id=seafarer['id'],
            name=seafarer['title'],
            department=get_or_create(Department, name=seafarer['department']),
            rank=get_or_create(Rank, name=seafarer['rank']),
            nationality=get_or_create(Nationality, name=seafarer['nationality'])
        ).execute()
        
        records = []
        for record in seafarer['service_records']:        
            records.append({
                ServiceRecord.seafarer: Seafarer.get_by_id(seafarer['id']),
                'department': get_or_create(Department, name=record['department']) if record.get('department') else None,
                'rank': get_or_create(Rank, name=record['rank']) if record.get('rank') else None,
                'ship_type': get_or_create(ShipType, name=record['ship_type']) if record.get('ship_type') else None,
                'vessel': get_or_create(Vessel, name=record.get('vessel_name'))
                    if record.get('vessel_name') else get_or_create(Vessel, href=record.get('vessel_href')),
                'company': get_or_create(Company, name=record['company']) if record.get('company') else None,
                'from_date': record['from'] if record.get('from') else None,
                'to_date': record['to'] if record.get('to') else None,
            })
        ServiceRecord.insert_many(rows=records).execute()

if __name__ == '__main__':
    from playhouse.migrate import migrate, MySQLMigrator

    migrator = MySQLMigrator(db)
    imo_number_field = IntegerField(null=True)
    ship_type_field = ForeignKeyField(ShipType, null=True)
    gross_tonnage_field = CharField()
    dwt_field = CharField()
    managerowner_field = ForeignKeyField(ManagerOwner, null=True)

    migrate(
        # migrator.add_column('vessel', 'imo_number', imo_number_field),
        migrator.add_column('vessel', 'ship_type', ship_type_field),
        migrator.add_column('vessel', 'gross_tonnage', gross_tonnage_field),
        migrator.add_column('vessel', 'dwt_field', dwt_field),
        migrator.add_column('vessel', 'managerowner', managerowner_field),
    )

print('proccess finished')

    