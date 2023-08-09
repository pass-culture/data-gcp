from sqlalchemy import create_engine, engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import os
from utils.env_vars import (
    SQL_BASE_USER,
    SQL_BASE_PASSWORD,
    SQL_BASE,
    SQL_CONNECTION_NAME
)

DATA_GCP_TEST_POSTGRES_PORT = os.getenv("DATA_GCP_TEST_POSTGRES_PORT", 5432)
# DB_NAME = os.getenv("DB_NAME", "postgres")

# DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

#DATABASE_URL = f"postgresql+pg8000://{SQL_BASE}:{SQL_BASE_PASSWORD}@/cloudsql/passculture-data-ehp:europe-west1:cloudsql-recommendation-dev-ew1" 
#DATABASE_URL = f"postgresql://{SQL_BASE}:{SQL_BASE_PASSWORD}@/cloudsql/passculture-data-ehp:europe-west1:cloudsql-recommendation-dev-ew1" 

# Créez le moteur SQLAlchemy
# engine = create_engine(DATABASE_URL)

query_string = dict({"host": "/cloudsql/{}".format(SQL_CONNECTION_NAME)})

bind_engine = create_engine( 
    engine.url.URL(
            drivername="postgres+psycopg2",
            username=SQL_BASE_USER,
            password=SQL_BASE_PASSWORD,
            database=SQL_BASE,
            query=query_string,
        ),
        pool_size=3,
        max_overflow=15,
        pool_timeout=30,
        pool_recycle=1800,
    )

#Session = sessionmaker(bind=bind_engine)

# Créez une classe de base pour les modèles SQLAlchemy
Base = declarative_base()

# Créez une classe Session pour gérer les sessions de base de données
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=bind_engine)
