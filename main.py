from src.extract_data import extract_weather_data
from src.transform_data import data_transformations
from src.load_data import load_weather_data
from pathlib import Path
import os
from dotenv import load_dotenv
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

env_path = Path(__file__).resolve().parent.parent / 'config' / '.env'
load_dotenv(env_path)

API_KEY = os.getenv("API_KEY")

url= f"https://api.openweathermap.org/data/2.5/weather?q=Recife,BR&units=metric&appid={API_KEY}"
table_name = 'weather_data_recife'

def pipeline():
    try:
        logging.info("Iniciando pipeline")
        logging.info("Extraindo dados")
        extract_weather_data(url)
        logging.info("Transformando dados")
        df_transformed = data_transformations()
        logging.info("Carregando dados")
        load_weather_data(table_name, df_transformed)
        logging.info("Pipeline finalizada com sucesso")
    except Exception as e:
        logging.error(f"Erro na pipeline: {e}")
        import traceback
        traceback.print_exc()

pipeline()