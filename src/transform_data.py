import pandas as pd
from pathlib import Path
import json

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

path_name = Path(__file__).parent.parent / 'data' / 'weather_data.json'

columns_to_drop = ['weather_icon', 'sys.type']

columns_names_to_rename = {
        "base": "base",
        "visibility": "visibility",
        "dt": "datetime",
        "timezone": "timezone",
        "id": "city_id", 
        "name": "city_name",
        "cod": "code",
        "coord.lon": "longitude",
        "coord.lat": "latitude",
        "main.temp": "temperature",
        "main.feels_like": "feels_like",
        "main.temp_min": "temp_min",
        "main.temp_max": "temp_max",
        "main.pressure": "pressure",
        "main.humidity": "humidity",
        "main.sea_level": "sea_level",
        "main.grnd_level": "grnd_level",
        "wind.speed": "wind_speed",
        "wind.deg": "wind_deg",
        "wind.gust": "wind_gust",
        "clouds.all": "clouds", 
        "sys.type": "sys_type",                 
        "sys.id": "sys_id",                
        "sys.country": "country",                
        "sys.sunrise": "sunrise",                
        "sys.sunset": "sunset",
        # weather_id, weather_main, weather_description 
    }

columns_name_normalize_datetime = ['datetime', 'sunrise', 'sunset']
def create_daframe(path_name:str) -> pd.DataFrame:
    
    logging.info("Criando dataframe a partir do arquivo json")

    path = path_name

    if not path.exists():
        raise FileNotFoundError(f"O arquivo {path} não existe")
    
    with open(path) as f:
        data = json.load(f)

    df = pd.json_normalize(data)

    logging.info(f"Dataframe criado com sucesso com {len(df)} linhas")

    return df

def normalize_weather_columns(df: pd.DataFrame) -> pd.DataFrame:
    df_weather = pd.json_normalize(df['weather'].apply(lambda x: x[0]))

    df_weather = df_weather.rename(columns={
        'id': 'weather_id',
        'main': 'weather_main',
        'description': 'weather_description',
        'icon': 'weather_icon'})
    
    df = pd.concat([df, df_weather], axis=1).drop(columns=['weather'])

    logging.info("Colunas de weather normalizadas")

    return df

def drop_columns(df: pd.DataFrame, columns_names:list[str]) -> pd.DataFrame:

    logging.info(f"Removendo colunas: {columns_names}")
    df = df.drop(columns = columns_names)

    logging.info(f"Colunas {columns_names} removidas")

    return df

def rename_columns(df: pd.DataFrame, columns_names:dict[str,str]) -> pd.DataFrame:
    df = df.rename(columns=columns_names)
    logging.info(f"Colunas renomeadas")
    return df

def normalize_datetime_columns(df:pd.DataFrame, columns_names:list[str]) -> pd.DataFrame:
    logging.info(f"Normalizando colunas de datetime: {columns_names}")
    for columns in columns_names:
        df[columns] = pd.to_datetime(df[columns], unit='s', utc=True).dt.tz_convert('America/Sao_Paulo')
    logging.info(f"Colunas de datetime normalizadas")
    return df

def data_transformations():
    print("Iniciando transformações dos dados")
    df = create_daframe(path_name)
    df = normalize_weather_columns(df)
    df = drop_columns(df, columns_to_drop)
    df = rename_columns(df, columns_names_to_rename)
    df = normalize_datetime_columns(df, columns_name_normalize_datetime)

    logging.info("Transformações realizadas com sucesso")

    return df