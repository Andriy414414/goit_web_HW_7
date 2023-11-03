import configparser
import pathlib

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# URI: postgresql://username:password@localhost:

file_config = pathlib.Path(__file__).parent.parent.joinpath('config.ini')
config = configparser.ConfigParser()
config.read(file_config)

user = config.get('DB', 'USER')
password = config.get('DB', 'PASSWORD')
domain = config.get('DB', 'DOMAIN')
port = config.get('DB', 'PORT')
db = config.get('DB', 'DB_NAME')

URI = f"postgresql://{user}:{password}@{domain}:{port}/{db}"

engine = create_engine(URI, echo=False, pool_size=5, max_overflow=0)
DBSession = sessionmaker(bind=engine)
session = DBSession()
