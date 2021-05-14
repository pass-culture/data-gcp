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
        "MODEL_REGION": "model_region",
        "MODEL_NAME": "model_name",
        "MODEL_VERSION": "model_version",
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
            "type": ["A", "B", "C", "D", "E", "B"],
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
            "department": ["93", "93", "93", "01", "93", "93"],
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
            "user_id": ["111", "112", "113"],
            "catch_up_user_id": [None, None, None],
            "pratique_artistique": [True, True, False],
            "autre": [True, False, False],
            "musees_patrimoine": [True, True, False],
            "spectacle_vivant": [True, False, False],
            "cinema": [True, True, False],
            "instrument": [True, False, False],
            "presse": [True, True, False],
            "audiovisuel": [True, False, False],
            "jeux_videos": [True, True, False],
            "livre": [True, False, False],
            "musique": [True, True, False],
        }
    )
    qpi_answers.to_sql("qpi_answers", con=engine, if_exists="replace")

    iris_venues_mv = pd.DataFrame(
        {"iris_id": ["1", "1", "1", "2"], "venue_id": ["11", "22", "33", "44"]}
    )
    iris_venues_mv.to_sql("iris_venues_mv", con=engine, if_exists="replace")

    ab_testing = pd.DataFrame({"userid": ["111", "112"], "groupid": ["A", "B"]})
    ab_testing.to_sql(app_config["AB_TESTING_TABLE"], con=engine, if_exists="replace")

    past_recommended_offers = pd.DataFrame(
        {"userid": [1], "offerid": [1], "date": [datetime.datetime.now(pytz.utc)]}
    )
    past_recommended_offers.to_sql(
        "past_recommended_offers", con=engine, if_exists="replace"
    )

    yield connection

    engine.execute("DROP TABLE IF EXISTS recommendable_offers;")
    engine.execute("DROP TABLE IF EXISTS non_recommendable_offers;")
    engine.execute("DROP TABLE IF EXISTS iris_venues;")
    engine.execute(f"DROP TABLE IF EXISTS {app_config['AB_TESTING_TABLE']} ;")
    engine.execute(f"DROP TABLE IF EXISTS past_recommended_offers ;")
    connection.close()
