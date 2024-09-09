import os
from sqlalchemy import text, create_engine, URL
import requests
import time
from dotenv import load_dotenv

load_dotenv()

url_dict = {
    "drivername":"postgresql+psycopg2",
    "username":os.getenv('PSQL_USER'),
    "password":os.getenv('PSQL_PASSWORD'),
    "host":os.getenv('DB_HOST'),
    "port":os.getenv('DB_PORT'),
    "database":os.getenv('DB_NAME')
}

db_url = URL.create(drivername=url_dict['drivername'], query=url_dict)

engine = create_engine(db_url)
engine.begin()