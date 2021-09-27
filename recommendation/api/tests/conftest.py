import datetime
import os
import pytest

import pandas as pd
import pytz
from sqlalchemy import create_engine
from typing import Any, Dict


DATA_GCP_TEST_POSTGRES_PORT = os.getenv("DATA_GCP_TEST_POSTGRES_PORT")
DB_NAME = os.getenv("DB_NAME")

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
        "AB_TESTING_TABLE": "ab_testing_202104_v0_v0bis",
        "NUMBER_OF_RECOMMENDATIONS": 10,
        "NUMBER_OF_PRESELECTED_OFFERS": 50,
        "MODEL_REGION": "model_region",
        "MODEL_NAME_A": "model_name",
        "MODEL_VERSION_A": "model_version",
        "MODEL_INPUT_A": "model_input",
        "MODEL_NAME_B": "model_name",
        "MODEL_VERSION_B": "model_version",
        "MODEL_INPUT_B": "model_input",
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
    recommendable_offers.to_sql("recommendable_offers", con=engine, if_exists="replace")

    non_recommendable_offers = pd.DataFrame(
        {"user_id": ["111", "112"], "offer_id": ["1", "3"]}
    )
    non_recommendable_offers.to_sql(
        "non_recommendable_offers", con=engine, if_exists="replace"
    )

    booking = pd.DataFrame(
        {"user_id": ["111", "111", "111", "112"], "offer_id": ["1", "3", "2", "1"]}
    )
    booking.to_sql("booking", con=engine, if_exists="replace")

    qpi_answers = pd.DataFrame(
        {
            "user_id": ["111", "112", "113", "114"],
            "catch_up_user_id": [None, None, None, None],
            "pratique_artistique": [True, True, False, False],
            "autre": [True, False, False, False],
            "musees_patrimoine": [True, True, False, False],
            "spectacle_vivant": [True, False, False, False],
            "cinema": [True, True, False, False],
            "instrument": [True, False, False, False],
            "presse": [True, True, False, False],
            "audiovisuel": [True, False, False, False],
            "jeux_videos": [True, True, False, False],
            "livre": [True, False, False, False],
            "musique": [True, True, False, False],
        }
    )
    qpi_answers.to_sql("qpi_answers", con=engine, if_exists="replace")

    iris_venues_mv = pd.DataFrame(
        {"iris_id": ["1", "1", "1", "2"], "venue_id": ["11", "22", "33", "44"]}
    )
    iris_venues_mv.to_sql("iris_venues_mv", con=engine, if_exists="replace")

    ab_testing = pd.DataFrame(
        {"userid": ["111", "112", "113"], "groupid": ["A", "B", "A"]}
    )
    ab_testing.to_sql(app_config["AB_TESTING_TABLE"], con=engine, if_exists="replace")

    past_recommended_offers = pd.DataFrame(
        {
            "userid": [1],
            "offerid": [1],
            "date": [datetime.datetime.now(pytz.utc)],
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

    yield connection

    engine.execute("DROP TABLE IF EXISTS recommendable_offers;")
    engine.execute("DROP TABLE IF EXISTS non_recommendable_offers;")
    engine.execute("DROP TABLE IF EXISTS iris_venues;")
    engine.execute(f"DROP TABLE IF EXISTS {app_config['AB_TESTING_TABLE']} ;")
    engine.execute("DROP TABLE IF EXISTS past_recommended_offers ;")
    engine.execute("DROP TABLE IF EXISTS number_of_bookings_per_user ;")
    connection.close()


@pytest.fixture
def setup_pool(app_config: Dict[str, Any]) -> Any:
    engine = create_engine(
        f"postgresql+psycopg2://postgres:postgres@127.0.0.1:{DATA_GCP_TEST_POSTGRES_PORT}/{DB_NAME}"
    )
    connection = engine.connect().execution_options(autocommit=True)
    recommendable_offers = pd.DataFrame(
        {
            "offer_id": ["1", "2", "3", "4", "5", "6"],
            "venue_id": ["11", "22", "33", "44", "55", "22"],
            "category": ["A", "B", "C", "D", "E", "B"],
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
    recommendable_offers.to_sql("recommendable_offers", con=engine, if_exists="replace")

    non_recommendable_offers = pd.DataFrame(
        {"user_id": ["111", "112"], "offer_id": ["1", "3"]}
    )
    non_recommendable_offers.to_sql(
        "non_recommendable_offers", con=engine, if_exists="replace"
    )

    booking = pd.DataFrame(
        {"user_id": ["111", "111", "111", "112"], "offer_id": ["1", "3", "2", "1"]}
    )
    booking.to_sql("booking", con=engine, if_exists="replace")

    qpi_answers = pd.DataFrame(
        {
            "user_id": ["111", "112", "113", "114"],
            "catch_up_user_id": [None, None, None, None],
            "pratique_artistique": [True, True, False, False],
            "autre": [True, False, False, False],
            "musees_patrimoine": [True, True, False, False],
            "spectacle_vivant": [True, False, False, False],
            "cinema": [True, True, False, False],
            "instrument": [True, False, False, False],
            "presse": [True, True, False, False],
            "audiovisuel": [True, False, False, False],
            "jeux_videos": [True, True, False, False],
            "livre": [True, False, False, False],
            "musique": [True, True, False, False],
        }
    )
    qpi_answers.to_sql("qpi_answers", con=engine, if_exists="replace")

    iris_venues_mv = pd.DataFrame(
        {"iris_id": ["1", "1", "1", "2"], "venue_id": ["11", "22", "33", "44"]}
    )
    iris_venues_mv.to_sql("iris_venues_mv", con=engine, if_exists="replace")

    ab_testing = pd.DataFrame(
        {"userid": ["111", "112", "113"], "groupid": ["A", "B", "A"]}
    )
    ab_testing.to_sql(app_config["AB_TESTING_TABLE"], con=engine, if_exists="replace")

    past_recommended_offers = pd.DataFrame(
        {
            "userid": [1],
            "offerid": [1],
            "date": [datetime.datetime.now(pytz.utc)],
            "group_id": "A",
            "reco_origin": "algo",
        }
    )
    past_recommended_offers.to_sql(
        "past_recommended_offers", con=engine, if_exists="replace"
    )

    number_of_bookings_per_user = pd.DataFrame(
        {"user_id": [111, 112, 113, 114], "bookings_count": [3, 3, 1, 1]}
    )
    number_of_bookings_per_user.to_sql(
        "number_of_bookings_per_user", con=engine, if_exists="replace"
    )

    yield engine

    engine.execute("DROP TABLE IF EXISTS recommendable_offers;")
    engine.execute("DROP TABLE IF EXISTS non_recommendable_offers;")
    engine.execute("DROP TABLE IF EXISTS iris_venues;")
    engine.execute(f"DROP TABLE IF EXISTS {app_config['AB_TESTING_TABLE']} ;")
    engine.execute("DROP TABLE IF EXISTS past_recommended_offers ;")
    engine.execute("DROP TABLE IF EXISTS number_of_bookings_per_user ;")
    connection.close()
