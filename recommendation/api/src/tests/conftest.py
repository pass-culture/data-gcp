from datetime import datetime, timedelta
import os
import pytest

import pandas as pd
import pytz
from sqlalchemy import create_engine
from typing import Any, Dict
from pcreco.utils.env_vars import AB_TESTING_TABLE


DATA_GCP_TEST_POSTGRES_PORT = os.getenv("DATA_GCP_TEST_POSTGRES_PORT")
DB_NAME = os.getenv("DB_NAME", "db")
ACTIVE_MODEL = os.environ.get("ACTIVE_MODEL")

TEST_DATABASE_CONFIG = {
    "user": "postgres",
    "password": "postgres",
    "host": "127.0.0.1",
    "port": DATA_GCP_TEST_POSTGRES_PORT,
    "database": DB_NAME,
}


@pytest.fixture
def app_config() -> Dict[str, Any]:
    return {
        "AB_TESTING_TABLE": AB_TESTING_TABLE,
        "NUMBER_OF_RECOMMENDATIONS": 10,
        "NUMBER_OF_PRESELECTED_OFFERS": 50,
        "MODEL_REGION": "model_region",
        "MODEL_NAME_A": "model_name",
        "MODEL_VERSION_A": "model_version",
        "MODEL_INPUT_A": "model_input",
        "MODEL_NAME_B": "model_name",
        "MODEL_VERSION_B": "model_version",
        "MODEL_INPUT_B": "model_input",
        "MODEL_NAME_C": "mf_reco",
        "MODEL_VERSION_C": "model_version",
        "MODEL_INPUT_C": "model_input",
    }


@pytest.fixture
def setup_database(app_config: Dict[str, Any]) -> Any:
    engine = create_engine(
        f"postgresql+psycopg2://postgres:postgres@127.0.0.1:{DATA_GCP_TEST_POSTGRES_PORT}/{DB_NAME}"
    )
    connection = engine.connect().execution_options(autocommit=True)
    recommendable_offers = pd.DataFrame(
        {
            "offer_id": ["1", "2", "3", "4", "5", "6"],
            "venue_id": ["11", "22", "33", "44", "55", "22"],
            "category": ["A", "B", "C", "D", "E", "B"],
            "subcategory_id": [
                "EVENEMENT_CINE",
                "EVENEMENT_CINE",
                "EVENEMENT_CINE",
                "EVENEMENT_CINE",
                "SPECTACLE_REPRESENTATION",
                "SPECTACLE_REPRESENTATION",
            ],
            "search_group_name": [
                "CINEMA",
                "CINEMA",
                "CINEMA",
                "CINEMA",
                "SPECTACLE",
                "SPECTACLE",
            ],
            "name": ["a", "b", "c", "d", "e", "f"],
            "url": [None, None, "url", "url", None, None],
            "is_national": [True, False, True, False, True, False],
            "booking_number": [3, 5, 10, 2, 1, 9],
            "item_id": [
                "offer-1",
                "offer-2",
                "offer-3",
                "offer-4",
                "offer-5",
                "offer-6",
            ],
            "product_id": [
                "product-1",
                "product-2",
                "product-3",
                "product-4",
                "product-5",
                "product-6",
            ],
        }
    )

    engine.execute("DROP MATERIALIZED VIEW IF EXISTS recommendable_offers CASCADE;")
    engine.execute(
        "DROP MATERIALIZED VIEW IF EXISTS recommendable_offers_eac_15 CASCADE;"
    )
    engine.execute(
        "DROP MATERIALIZED VIEW IF EXISTS recommendable_offers_eac_16_17 CASCADE;"
    )

    recommendable_offers.to_sql(
        "recommendable_offers_temporary_table", con=engine, if_exists="replace"
    )
    engine.execute(
        "CREATE MATERIALIZED VIEW recommendable_offers AS SELECT * FROM recommendable_offers_temporary_table WITH DATA;"
    )

    engine.execute(
        "CREATE MATERIALIZED VIEW recommendable_offers_eac_15 AS SELECT * FROM recommendable_offers_temporary_table WITH DATA;"
    )

    engine.execute(
        "CREATE MATERIALIZED VIEW recommendable_offers_eac_16_17 AS SELECT * FROM recommendable_offers_temporary_table WITH DATA;"
    )

    non_recommendable_offers = pd.DataFrame(
        {"user_id": ["111", "112"], "offer_id": ["1", "3"]}
    )
    engine.execute("DROP MATERIALIZED VIEW IF EXISTS non_recommendable_offers CASCADE;")

    non_recommendable_offers.to_sql(
        "non_recommendable_offers_temporary_table", con=engine, if_exists="replace"
    )
    engine.execute(
        "CREATE MATERIALIZED VIEW non_recommendable_offers AS SELECT * FROM non_recommendable_offers_temporary_table;"
    )

    booking = pd.DataFrame(
        {"user_id": ["111", "111", "111", "112"], "offer_id": ["1", "3", "2", "1"]}
    )
    booking.to_sql("booking", con=engine, if_exists="replace")

    enriched_user = pd.DataFrame(
        {
            "user_id": ["111", "112", "113", "114", "115", "116", "117", "118"],
            "user_deposit_creation_date": [datetime.now(pytz.utc)] * 8,
            "user_birth_date": [
                (datetime.now() - timedelta(days=18 * 366)),
                (datetime.now() - timedelta(days=18 * 366)),
                (datetime.now() - timedelta(days=18 * 366)),
                (datetime.now() - timedelta(days=18 * 366)),
                (datetime.now() - timedelta(days=15 * 366)),
                (datetime.now() - timedelta(days=16 * 366)),
                (datetime.now() - timedelta(days=17 * 366)),
                (datetime.now() - timedelta(days=18 * 366)),
            ],
            "user_deposit_initial_amount": [300, 300, 300, 300, 20, 30, 30, 300],
        }
    )
    enriched_user.to_sql("enriched_user", con=engine, if_exists="replace")

    qpi_answers = pd.DataFrame(
        {
            "user_id": ["111", "112", "113", "114"],
            "catch_up_user_id": [None, None, None, None],
            "SUPPORT_PHYSIQUE_FILM": [False, False, True, True],
            "ABO_MEDIATHEQUE": [False, True, True, False],
            "VOD": [False, False, True, False],
            "ABO_PLATEFORME_VIDEO": [True, True, True, True],
            "AUTRE_SUPPORT_NUMERIQUE": [False, False, False, True],
            "CARTE_CINE_MULTISEANCES": [True, False, True, False],
            "CARTE_CINE_ILLIMITE": [True, True, True, False],
            "SEANCE_CINE": [True, True, True, False],
            "EVENEMENT_CINE": [False, True, False, True],
            "FESTIVAL_CINE": [False, True, False, False],
            "CINE_VENTE_DISTANCE": [True, True, True, True],
            "CINE_PLEIN_AIR": [True, False, False, False],
            "CONFERENCE": [False, False, False, False],
            "RENCONTRE": [True, False, True, True],
            "DECOUVERTE_METIERS": [True, True, False, False],
            "SALON": [False, True, True, True],
            "CONCOURS": [False, True, False, True],
            "RENCONTRE_JEU": [False, False, True, False],
            "ESCAPE_GAME": [False, False, False, True],
            "EVENEMENT_JEU": [True, True, True, False],
            "JEU_EN_LIGNE": [True, False, False, False],
            "ABO_JEU_VIDEO": [True, True, False, True],
            "ABO_LUDOTHEQUE": [False, True, False, True],
            "LIVRE_PAPIER": [True, True, False, True],
            "LIVRE_NUMERIQUE": [False, True, False, True],
            "TELECHARGEMENT_LIVRE_AUDIO": [False, False, False, True],
            "LIVRE_AUDIO_PHYSIQUE": [False, False, True, False],
            "ABO_BIBLIOTHEQUE": [True, True, True, False],
            "ABO_LIVRE_NUMERIQUE": [True, False, False, False],
            "FESTIVAL_LIVRE": [False, True, True, True],
            "CARTE_MUSEE": [True, False, True, False],
            "ABO_MUSEE": [False, False, False, True],
            "VISITE": [False, False, True, True],
            "VISITE_GUIDEE": [False, False, True, True],
            "EVENEMENT_PATRIMOINE": [True, False, False, True],
            "VISITE_VIRTUELLE": [False, False, False, True],
            "MUSEE_VENTE_DISTANCE": [False, True, False, True],
            "CONCERT": [True, True, True, True],
            "EVENEMENT_MUSIQUE": [True, True, True, True],
            "LIVESTREAM_MUSIQUE": [False, False, False, True],
            "ABO_CONCERT": [False, False, True, False],
            "FESTIVAL_MUSIQUE": [True, True, False, True],
            "SUPPORT_PHYSIQUE_MUSIQUE": [False, True, True, False],
            "TELECHARGEMENT_MUSIQUE": [False, False, True, False],
            "ABO_PLATEFORME_MUSIQUE": [False, False, False, False],
            "CAPTATION_MUSIQUE": [True, False, True, False],
            "SEANCE_ESSAI_PRATIQUE_ART": [True, False, False, True],
            "ATELIER_PRATIQUE_ART": [True, True, False, False],
            "ABO_PRATIQUE_ART": [True, False, True, True],
            "ABO_PRESSE_EN_LIGNE": [True, False, True, False],
            "PODCAST": [True, True, False, True],
            "APP_CULTURELLE": [False, False, False, False],
            "SPECTACLE_REPRESENTATION": [True, False, True, False],
            "SPECTACLE_ENREGISTRE": [False, True, True, False],
            "LIVESTREAM_EVENEMENT": [True, True, True, False],
            "FESTIVAL_SPECTACLE": [True, True, False, False],
            "ABO_SPECTACLE": [False, True, True, True],
            "ACHAT_INSTRUMENT": [False, False, False, False],
            "BON_ACHAT_INSTRUMENT": [True, False, True, True],
            "LOCATION_INSTRUMENT": [False, True, False, False],
            "PARTITION": [False, False, False, True],
            "MATERIEL_ART_CREATIF": [True, False, False, False],
            "ACTIVATION_EVENT": [False, False, True, False],
            "ACTIVATION_THING": [True, True, False, True],
            "JEU_SUPPORT_PHYSIQUE": [True, True, False, True],
            "OEUVRE_ART": [True, True, False, False],
        }
    )
    qpi_answers.to_sql("qpi_answers", con=engine, if_exists="replace")

    iris_venues_mv = pd.DataFrame(
        {"iris_id": ["1", "1", "1", "2"], "venue_id": ["11", "22", "33", "44"]}
    )
    iris_venues_mv.to_sql("iris_venues_mv", con=engine, if_exists="replace")

    ab_testing = pd.DataFrame(
        {
            "userid": ["111", "112", "113", "114", "115", "116", "117", "118"],
            "groupid": ["A", "B", "C", "B", "A", "B", "C", "B"],
        }
    )
    ab_testing.to_sql(app_config["AB_TESTING_TABLE"], con=engine, if_exists="replace")

    past_recommended_offers = pd.DataFrame(
        {
            "userid": [1],
            "offerid": [1],
            "date": [datetime.now(pytz.utc)],
            "group_id": "A",
            "reco_origin": "algo",
        }
    )
    past_recommended_offers.to_sql(
        "past_recommended_offers", con=engine, if_exists="replace"
    )

    number_of_bookings_per_user = pd.DataFrame(
        {"user_id": [111], "bookings_count": [3]},
        {"user_id": [113], "bookings_count": [1]},
        {"user_id": [114], "bookings_count": [1]},
    )
    number_of_bookings_per_user.to_sql(
        "number_of_bookings_per_user", con=engine, if_exists="replace"
    )

    firebase_events = pd.DataFrame(
        {
            "user_id": ["111", "111", "112", "112", "113", "113"],
            "offer_id": ["1", "2", "1", "1", "2", "3"],
            "event_date": [datetime.now(pytz.utc)] * 6,
            "event_name": [
                "ConsultOffer",
                "ConsultOffer",
                "ConsultOffer",
                "HasAddedOfferToFavorites",
                "ConsultOffer",
                "HasAddedOfferToFavorites",
            ],
        }
    )
    firebase_events.to_sql("firebase_events", con=engine, if_exists="replace")

    number_of_clicks_per_user = pd.DataFrame(
        {"user_id": ["111"], "clicks_count": [2]},
        {"user_id": ["112"], "clicks_count": [1]},
        {"user_id": ["113"], "clicks_count": [1]},
    )
    number_of_clicks_per_user.to_sql(
        "number_of_clicks_per_user", con=engine, if_exists="replace"
    )

    number_of_favorites_per_user = pd.DataFrame(
        {"user_id": ["111"], "favorites_count": [0]},
        {"user_id": ["112"], "favorites_count": [1]},
        {"user_id": ["113"], "favorites_count": [1]},
    )
    number_of_favorites_per_user.to_sql(
        "number_of_favorites_per_user", con=engine, if_exists="replace"
    )

    trained_users_mf_reco = pd.DataFrame({"user_id": ["111", "113"]})
    trained_users_mf_reco.to_sql(
        "trained_users_mf_reco", con=engine, if_exists="replace"
    )

    iris_france = pd.read_csv("./src/tests/iris_france_tests.csv")
    iris_france.to_sql("iris_france", con=engine, if_exists="replace", index=False)

    sql = """ALTER TABLE public.iris_france
            ALTER COLUMN shape TYPE Geometry(GEOMETRY, 4326)
            USING ST_SetSRID(shape::Geometry, 4326);
        """

    connection.execute(sql)

    yield connection

    engine.execute("DROP MATERIALIZED VIEW IF EXISTS recommendable_offers CASCADE;")
    engine.execute(
        "DROP MATERIALIZED VIEW IF EXISTS recommendable_offers_eac_15 CASCADE;"
    )
    engine.execute(
        "DROP MATERIALIZED VIEW IF EXISTS recommendable_offers_eac_16_17 CASCADE;"
    )
    engine.execute("DROP MATERIALIZED VIEW IF EXISTS non_recommendable_offers CASCADE;")
    engine.execute("DROP TABLE IF EXISTS recommendable_offers_temporary_table CASCADE;")
    engine.execute(
        "DROP TABLE IF EXISTS non_recommendable_offers_temporary_table CASCADE;"
    )
    engine.execute("DROP TABLE IF EXISTS iris_venues;")
    engine.execute(f"DROP TABLE IF EXISTS {app_config['AB_TESTING_TABLE']} ;")
    engine.execute("DROP TABLE IF EXISTS past_recommended_offers ;")
    engine.execute("DROP TABLE IF EXISTS number_of_bookings_per_user ;")
    engine.execute("DROP TABLE IF EXISTS number_of_clicks_per_user ;")
    engine.execute("DROP TABLE IF EXISTS number_of_favorites_per_user ;")
    engine.execute("DROP TABLE IF EXISTS iris_france;")
    connection.close()
