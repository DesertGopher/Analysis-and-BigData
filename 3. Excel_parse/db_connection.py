from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

user = 'igor'
password = 'igor'
host = '127.0.0.1'
port = 5432
database = 'calls'

engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}", pool_size=100, max_overflow=0)


def get_session():
    session = sessionmaker(
        engine,
        autocommit=False,
        autoflush=False,
    )
    return session()
