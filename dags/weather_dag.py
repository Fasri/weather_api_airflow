from datetime import datetime, timedelta
from airflow.decorators import dag, task
from pathlib import Path
import sys, os
from dotenv import load_dotenv

#sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
sys.path.insert(0, '/opt/airflow/src')

from extract_data import extract_weather_data
from transform_data import data_transformations 
from load_data import load_weather_data

env_path = Path(__file__).resolve().parent.parent / 'config' / '.env'
load_dotenv(dotenv_path=env_path)

API_KEY = os.getenv("API_KEY")

url= f"https://api.openweathermap.org/data/2.5/weather?q=Recife,BR&units=metric&appid={API_KEY}"

@dag(
    dag_id='weather_etl_dag',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
    },
    description='ETL pipeline para dados meteorológicos de Recife',
    schedule = '*/2 * * * *', 
    start_date=datetime(2026, 2, 24),
    catchup=False,
    tags=['recife', 'weather']
)

def weather_etl_dag():

    @task()
    def extract():
        extract_weather_data(url)

    @task()
    def transform():
        df = data_transformations()
        df.to_parquet('/opt/airflow/data/weather_data_recife.parquet', index=False)

    @task()
    def load():
        import pandas as pd
        df = pd.read_parquet('/opt/airflow/data/weather_data_recife.parquet')
        load_weather_data('weather_data_recife', df)

    extract() >> transform() >> load()

weather_etl_dag()