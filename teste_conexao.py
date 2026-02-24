from sqlalchemy import create_engine
from sqlalchemy.engine import URL

def get_engine():
    url = URL.create(
        "postgresql+psycopg2",
        username="felipe",
        password="230928",
        host="localhost",
        port=5432,
        database="felipe_weather"
    )

    return create_engine(url, connect_args={'client_encoding': 'utf8'})

engine = get_engine()

with engine.connect() as conn:
    print("Conectado com sucesso!")
