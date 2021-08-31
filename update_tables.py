import requests
import zipfile
import os
import shutil
import re
import sqlalchemy.exc

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from pathlib import Path

load_dotenv()

url = os.getenv('ZIP_URL')
name = os.getenv('ZIP_NAME')
mysql_folder = os.getenv('SQL_FOLDER')
db = create_engine(os.getenv('POSTGRES_URL2'))


def download_zip(zip_url, zip_name):
    r = requests.get(zip_url, allow_redirects=True)
    open(zip_name, '+wb').write(r.content)


def unzip_file(zip_name):
    with zipfile.ZipFile(zip_name, 'r') as zip_ref:
        zip_ref.extractall()

    try:
        os.remove(zip_name)
    except FileNotFoundError:
        print('No such file.')


def get_mysql_list(folder):
    return [x for x in os.listdir(f'./{folder}') if x[-3:] == 'sql']


def convert_to_postgres(sql_folder, sql_name):
    with open(f'./{sql_folder}/{sql_name}', encoding='UTF-8') as file:
        new_string = file.read()
        query_list = new_string.split('\n')
        create_query_list = query_list[0].split(' ')
        id_name = None

        for query in create_query_list:
            result = re.search(r"KEY\('\w+'", query)
            if result:
                result_list = result.string[4:-3].split(',')
                print(f'{len(result_list)}: {result_list}')
                result_list = [i[1:-1] for i in result.string[4:-3].split(',') if (('id' in i and len(result_list) >= 1) or ('id' not in i))]
                print(result_list)
                id_name = result.string[4:-3].split(',').pop(0)[1:-1]

        insert_list = [insert[:-1] + f' ON CONFLICT ({id_name}) DO NOTHING;' for insert in query_list[1:-1] if 'INSERT INTO' in insert]
        new_string = f'{query_list[0]}\n' + '\n'.join(insert_list)

        new_string = new_string.replace("'", "")
        new_string = new_string.replace("`", "")
        new_string = new_string.replace('"', "'")
        new_string = new_string.replace('CREATE TABLE', 'CREATE TABLE IF NOT EXISTS')
        Path('./sql').mkdir(parents=True, exist_ok=True)
        open(f'sql/{sql_name}', '+wb').write(new_string.encode())

    try:
        os.remove(f'./{sql_folder}/{sql_name}')
    except FileNotFoundError:
        print('No such file.')


def create_tables(directory):
    if not os.listdir(f'./{directory}'):
        raise IsADirectoryError

    for filename in os.listdir(f'./{directory}'):
        with open(f'./{directory}/{filename}', encoding='UTF-8') as file:
            sql_query = file.read()

            try:
                db.execute(text(sql_query))
            except (sqlalchemy.exc.ProgrammingError, sqlalchemy.exc.DataError) as e:
                pass
                # print(e)


def update_tables():
    pass


if __name__ == '__main__':
    download_zip(url, name)
    unzip_file(name)
    sql_list = get_mysql_list(mysql_folder)
    for sql in sql_list:
        convert_to_postgres(mysql_folder, sql)

    shutil.rmtree(f'./{mysql_folder}')

    try:
        pass
        # create_tables('./sql')
    except IsADirectoryError:
        print('No such directory.')
