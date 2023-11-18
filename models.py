from os import getenv

if getenv("HOST") is None:
    from dotenv import load_dotenv
    load_dotenv()

host = getenv('POSTGRES_HOST')
database = getenv('POSTGRES_DB')
user = getenv('POSTGRES_USER')
pg_port = getenv('POSTGRES_PORT')
pg_pass = getenv('POSTGRES_PWD')

from sqlalchemy import create_engine
from sqlalchemy.sql import text


def get_connection():
    return create_engine(url=f"postgresql://{user}:{pg_pass}@{host}:{pg_port}/{database}") 

con = get_connection()