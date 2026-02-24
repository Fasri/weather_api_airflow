from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
import os
from pathlib import Path
import pandas as pd

from dotenv import load_dotenv

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

env_path = Path(__file__).resolve().parent.parent / 'config' / '.env'

load_dotenv(dotenv_path=env_path)

DB_USER = os.getenv("user")
DB_PASSWORD = os.getenv("password")
DB_HOST = 'host.docker.internal'
DB_PORT = 5432
DB_NAME = os.getenv("database")

def get_engine():

    logging.info("Criando engine")
    return create_engine(f"postgresql+psycopg2://{DB_USER}:{quote_plus(DB_PASSWORD)}@{DB_HOST}:{DB_PORT}/{DB_NAME}", connect_args={'client_encoding': 'utf8'})

engine = get_engine()

def load_weather_data(table_name:str, df:pd.DataFrame):
    logging.info(f"Carregando dados para a tabela {table_name}")
    df.to_sql(name = table_name, con=engine, if_exists='append', index=False)
    logging.info(f"Dados carregados com sucesso para a tabela {table_name}")

    df_check = pd.read_sql(f'SELECT * FROM {table_name}', con=engine)
    logging.info(f"Dados carregados na tabela {table_name}:\n{df_check.head()}")