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
    recommendable_offers_per_iris_shape = pd.DataFrame(
        {
            "item_id": [
                "isbn-1",
                "isbn-2",
                "movie-3",
                "movie-4",
                "movie-5",
                "product-6",
            ],
            "offer_id": ["1", "2", "3", "4", "5", "6"],
            "product_id": ["1", "2", "3", "4", "5", "6"],
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
            "iris_id": ["11", "22", "33", "44", "55", "22"],
            "venue_id": ["11", "22", "33", "44", "55", "22"],
            "venue_distance_to_iris": [11, 22, 33, 44, 55, 22],
            "name": ["a", "b", "c", "d", "e", "f"],
            "is_numerical": [False, False, True, True, False, False],
            "is_national": [True, False, True, False, True, False],
            "is_geolocated": [False, True, False, False, False, True],
            "booking_number": [3, 5, 10, 2, 1, 9],
            "offer_creation_date": [
                "2020-01-01",
                "2020-01-01",
                "2020-01-01",
                "2020-01-01",
                "2020-01-01",
                "2020-01-01",
            ],
            "stock_creation_date": [
                "2020-01-01",
                "2020-01-01",
                "2020-01-01",
                "2020-01-01",
                "2020-01-01",
                "2020-01-01",
            ],
            "stock_price": [10, 20, 20, 30, 30, 30],
            "booking_number": [1, 2, 3, 4, 5, 6],
            "is_underage_recommendable": [True, True, True, False, False, False],
            "position": ["in", "out", "in", "out", "in", "out"],
            "unique_id": [1, 2, 3, 4, 5, 6],
        }
    )

    engine.execute(
        "DROP MATERIALIZED VIEW IF EXISTS recommendable_offers_per_iris_shape_mv CASCADE;"
    )
    engine.execute(
        "DROP MATERIALIZED VIEW IF EXISTS recommendable_offers_per_iris_shape_eac_15_mv CASCADE;"
    )
    engine.execute(
        "DROP MATERIALIZED VIEW IF EXISTS recommendable_offers_per_iris_shape_eac_16_17_mv CASCADE;"
    )

    recommendable_offers_per_iris_shape.to_sql(
        "recommendable_offers_temporary_table", con=engine, if_exists="replace"
    )
    engine.execute(
        "CREATE MATERIALIZED VIEW recommendable_offers_per_iris_shape_mv AS SELECT * FROM recommendable_offers_temporary_table WITH DATA;"
    )

    engine.execute(
        "CREATE MATERIALIZED VIEW recommendable_offers_per_iris_shape_eac_15_mv AS SELECT * FROM recommendable_offers_temporary_table WITH DATA;"
    )

    engine.execute(
        "CREATE MATERIALIZED VIEW recommendable_offers_per_iris_shape_eac_16_17_mv AS SELECT * FROM recommendable_offers_temporary_table WITH DATA;"
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
            "user_theoretical_remaining_credit": [300, 300, 300, 300, 20, 30, 30, 300],
            "booking_cnt": [3, 1, 1, 3, 3, 4, 4, 4],
            "consult_offer": [1, 2, 2, 3, 3, 4, 4, 4],
            "has_added_offer_to_favorites": [1, 2, 2, 3, 3, 4, 4, 4],
        }
    )
    enriched_user.to_sql("enriched_user", con=engine, if_exists="replace")
    engine.execute(
        "CREATE MATERIALIZED VIEW enriched_user_mv AS SELECT * FROM enriched_user;"
    )
    qpi_answers = pd.DataFrame(
        {
            "user_id": ["111", "111", "112", "113", "114"],
            "subcategories": [
                "SUPPORT_PHYSIQUE_FILM",
                "JEU_EN_LIGNE",
                "SUPPORT_PHYSIQUE_FILM",
                "LIVRE_PAPIER",
                "LIVRE_PAPIER",
            ],
            "catch_up_user_id": [None, None, None, None, None],
        }
    )
    qpi_answers.to_sql("qpi_answers_mv", con=engine, if_exists="replace")

    iris_venues_mv = pd.DataFrame(
        {
            "iris_id": ["1", "1", "1", "2"],
            "venue_id": ["11", "22", "33", "44"],
            "venue_latitude": [2.43, 2.46, 2.46, 2.49],
            "venue_longitude": [48.810, 48.820, 48.830, 48.840],
        }
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
    engine.execute("DROP TABLE IF EXISTS enriched_user CASCADE;")
    engine.execute("DROP MATERIALIZED VIEW IF EXISTS enriched_user_mv CASCADE;")
    engine.execute("DROP TABLE IF EXISTS iris_venues;")
    engine.execute(f"DROP TABLE IF EXISTS {app_config['AB_TESTING_TABLE']} ;")
    engine.execute("DROP TABLE IF EXISTS past_recommended_offers ;")
    engine.execute("DROP TABLE IF EXISTS iris_france;")

    connection.close()
