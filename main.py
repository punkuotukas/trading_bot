import os
from sqlalchemy import text, create_engine
import requests
import time
from dotenv import load_dotenv

load_dotenv()

engine_dict = {
    'prefix':'postgresql+psycopg2://',
    'psql_user': os.getenv('PSQL_USER'),
    'psql_password': os.getenv('PSQL_PASSWORD'),
    'db_host': os.getenv('DB_HOST'),
    'db_port': os.getenv('DB_PORT'),
    'db_name': os.getenv('DB_NAME')
}
