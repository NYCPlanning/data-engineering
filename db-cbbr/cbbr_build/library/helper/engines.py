from dotenv import load_dotenv, find_dotenv
from pathlib import Path
from sqlalchemy import create_engine
from urllib.parse import urlparse
import psycopg2
import os

load_dotenv(Path(__file__).parent.parent / ".env")

recipe_engine = create_engine(os.getenv("RECIPE_ENGINE"))
edm_engine = create_engine(os.getenv("EDM_DATA"))
build_engine = create_engine(os.getenv("BUILD_ENGINE"))


def psycopg2_connect(url):
    result = urlparse(str(url))
    username = result.username
    password = result.password
    database = result.path[1:]
    hostname = result.hostname
    port = result.port
    connection = psycopg2.connect(
        database=database, user=username, password=password, host=hostname, port=port
    )
    return connection
