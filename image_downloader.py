from sqlalchemy import create_engine
from dotenv import load_dotenv
from os import getenv
import requests

load_dotenv()


def get_unit_ids():
    db = create_engine(getenv('POSTGRES_URL'))
    results = db.execute('SELECT unit_id FROM unit_profile').fetchall()
    return [result for tuples in results for result in tuples]


def download_images():
    unit_ids = get_unit_ids()
    for unit_id in unit_ids:
        download_image(unit_id, 3)
        download_image(unit_id, 6)


def download_image(unit_id, star):
    url = f'https://redive.estertion.win/icon/unit/{unit_id + star*10}.webp'
    r = requests.get(url, allow_redirects=True)
    if r:
        open(f'img/avatars/{unit_id + star*10}.webp', '+wb').write(r.content)


download_images()
