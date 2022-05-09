import os
import time
from typing import Any

from access_gcp_secrets import access_secret
from sqlalchemy import create_engine, engine, text
from loguru import logger

GCP_PROJECT = os.environ.get("GCP_PROJECT")

SQL_BASE = os.environ.get("SQL_BASE")
SQL_BASE_USER = os.environ.get("SQL_BASE_USER")
SQL_BASE_SECRET_ID = os.environ.get("SQL_BASE_SECRET_ID")
SQL_BASE_SECRET_VERSION = os.environ.get("SQL_BASE_SECRET_VERSION")
SQL_CONNECTION_NAME = os.environ.get("SQL_CONNECTION_NAME")

SQL_BASE_PASSWORD = access_secret(
    GCP_PROJECT, SQL_BASE_SECRET_ID, SQL_BASE_SECRET_VERSION
)

AB_TESTING_TABLE = os.environ.get(
    "AB_TESTING_TABLE", "ab_testing"
)  # "ab_testing" for tests in circle ci
AB_TESTING_TABLE_EAC = os.environ.get("AB_TESTING_TABLE_EAC")
NUMBER_OF_RECOMMENDATIONS = 10
SHUFFLE_RECOMMENDATION = False
NUMBER_OF_PRESELECTED_OFFERS = 50 if not os.environ.get("CI") else 3

ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "dev")

MODEL_REGION = os.environ.get("MODEL_REGION")
MODEL_NAME_A = os.environ.get("MODEL_NAME_A")
MODEL_NAME_B = os.environ.get("MODEL_NAME_B")
MODEL_NAME_C = os.environ.get("MODEL_NAME_C")

MACRO_CATEGORIES_TYPE_MAPPING = {
    "SUPPORT_PHYSIQUE_FILM": ["FILM"],
    "ABO_MEDIATHEQUE": ["FILM"],
    "VOD": ["FILM"],
    "ABO_PLATEFORME_VIDEO": ["FILM"],
    "AUTRE_SUPPORT_NUMERIQUE": ["FILM"],
    "CARTE_CINE_MULTISEANCES": ["CINEMA"],
    "CARTE_CINE_ILLIMITE": ["CINEMA"],
    "SEANCE_CINE": ["CINEMA"],
    "EVENEMENT_CINE": ["CINEMA"],
    "FESTIVAL_CINE": ["CINEMA"],
    "CINE_VENTE_DISTANCE": ["CINEMA"],
    "CINE_PLEIN_AIR": ["CINEMA"],
    "CONFERENCE": ["CONFERENCE_RENCONTRE"],
    "RENCONTRE": ["CONFERENCE_RENCONTRE"],
    "DECOUVERTE_METIERS": ["CONFERENCE_RENCONTRE"],
    "SALON": ["CONFERENCE_RENCONTRE"],
    "CONCOURS": ["JEU"],
    "RENCONTRE_JEU": ["JEU"],
    "ESCAPE_GAME": ["JEU"],
    "EVENEMENT_JEU": ["JEU"],
    "JEU_EN_LIGNE": ["JEU"],
    "ABO_JEU_VIDEO": ["JEU"],
    "ABO_LUDOTHEQUE": ["JEU"],
    "LIVRE_PAPIER": ["LIVRE"],
    "LIVRE_NUMERIQUE": ["LIVRE"],
    "TELECHARGEMENT_LIVRE_AUDIO": ["LIVRE"],
    "LIVRE_AUDIO_PHYSIQUE": ["LIVRE"],
    "ABO_BIBLIOTHEQUE": ["LIVRE"],
    "ABO_LIVRE_NUMERIQUE": ["LIVRE"],
    "FESTIVAL_LIVRE": ["LIVRE"],
    "CARTE_MUSEE": ["MUSEE"],
    "ABO_MUSEE": ["MUSEE"],
    "VISITE": ["MUSEE"],
    "VISITE_GUIDEE": ["MUSEE"],
    "EVENEMENT_PATRIMOINE": ["MUSEE"],
    "VISITE_VIRTUELLE": ["MUSEE"],
    "MUSEE_VENTE_DISTANCE": ["MUSEE"],
    "CONCERT": ["MUSIQUE_LIVE"],
    "EVENEMENT_MUSIQUE": ["MUSIQUE_LIVE"],
    "LIVESTREAM_MUSIQUE": ["MUSIQUE_LIVE"],
    "ABO_CONCERT": ["MUSIQUE_LIVE"],
    "FESTIVAL_MUSIQUE": ["MUSIQUE_LIVE"],
    "SUPPORT_PHYSIQUE_MUSIQUE": ["MUSIQUE_ENREGISTREE"],
    "TELECHARGEMENT_MUSIQUE": ["MUSIQUE_ENREGISTREE"],
    "ABO_PLATEFORME_MUSIQUE": ["MUSIQUE_ENREGISTREE"],
    "CAPTATION_MUSIQUE": ["MUSIQUE_ENREGISTREE"],
    "SEANCE_ESSAI_PRATIQUE_ART": ["PRATIQUE_ART"],
    "ATELIER_PRATIQUE_ART": ["PRATIQUE_ART"],
    "ABO_PRATIQUE_ART": ["PRATIQUE_ART"],
    "ABO_PRESSE_EN_LIGNE": ["MEDIA"],
    "PODCAST": ["MEDIA"],
    "APP_CULTURELLE": ["MEDIA"],
    "SPECTACLE_REPRESENTATION": ["SPECTACLE"],
    "SPECTACLE_ENREGISTRE": ["SPECTACLE"],
    "LIVESTREAM_EVENEMENT": ["SPECTACLE"],
    "FESTIVAL_SPECTACLE": ["SPECTACLE"],
    "ABO_SPECTACLE": ["SPECTACLE"],
    "ACHAT_INSTRUMENT": ["INSTRUMENT"],
    "BON_ACHAT_INSTRUMENT": ["INSTRUMENT"],
    "LOCATION_INSTRUMENT": ["INSTRUMENT"],
    "PARTITION": ["INSTRUMENT"],
    "MATERIEL_ART_CREATIF": ["BEAUX_ARTS"],
    "ACTIVATION_EVENT": ["TECHNIQUE"],
    "ACTIVATION_THING": ["TECHNIQUE"],
    "JEU_SUPPORT_PHYSIQUE": ["TECHNIQUE"],
    "OEUVRE_ART": ["TECHNIQUE"],
}

query_string = dict(
    {"unix_sock": "/cloudsql/{}/.s.PGSQL.5432".format(SQL_CONNECTION_NAME)}
)


def create_pool():
    return create_engine(
        engine.url.URL(
            drivername="postgres+pg8000",
            username=SQL_BASE_USER,
            password=SQL_BASE_PASSWORD,
            database=SQL_BASE,
            query=query_string,
        ),
        pool_size=20,
        max_overflow=2,
        pool_timeout=30,
        pool_recycle=1800,
    )


def create_db_connection() -> Any:
    return create_pool().connect().execution_options(autocommit=True)


def log_duration(message, start):
    logger.info(f"{message}: {time.time() - start} seconds.")


class PlaylistArgs:
    def __init__(self, json):
        self.start_date = json.get("start_date", None)
        self.end_date = json.get("end_date", None)
        self.isevent = json.get("isevent", None)
        self.categories = json.get("categories", None)
        self.subcategories = json.get("subcategories", None)
        self.price_max = json.get("price_max", None)

    def get_conditions(self):
        condition = ""
        if self.start_date:
            if self.isevent:
                column = "stock_beginning_date"
            else:
                column = "offer_creation_date"
            condition += f"""AND ({column} > '{self.start_date}' AND {column} < '{self.end_date}') \n"""
        if self.categories:
            condition += (
                "AND ("
                + " OR ".join([f"category='{cat}'" for cat in self.categories])
                + ")\n"
            )
        if self.subcategories:
            condition += (
                "AND ("
                + " OR ".join([f"subcategory_id='{cat}'" for cat in self.subcategories])
                + ")\n"
            )
        if self.price_max:
            condition += f"AND stock_price<={self.price_max} \n"
        return condition
