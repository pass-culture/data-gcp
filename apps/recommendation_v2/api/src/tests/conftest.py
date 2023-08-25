import os
from datetime import datetime, timedelta
import pandas as pd
import pytest
import pytz
from sqlalchemy import create_engine, text
from typing import Any, Dict


DATA_GCP_TEST_POSTGRES_PORT = os.getenv("DATA_GCP_TEST_POSTGRES_PORT")
DB_NAME = os.getenv("DB_NAME", "postgres")
DEFAULT_IRIS_ID = "45327"

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
        "NUMBER_OF_RECOMMENDATIONS": 10,
        "NUMBER_OF_PRESELECTED_OFFERS": 50,
        "MODEL_REGION": "model_region",
    }


def create_non_recommendable_offers(engine):
    non_recommendable_offers = pd.DataFrame(
        {"user_id": ["111", "112"], "offer_id": ["1", "3"]}
    )
    engine.connect().execute(
        text("DROP MATERIALIZED VIEW IF EXISTS non_recommendable_offers CASCADE;")
    )

    non_recommendable_offers.to_sql(
        "non_recommendable_offers_temporary_table", con=engine, if_exists="replace"
    )
    engine.connect().execute(
        text(
            "CREATE MATERIALIZED VIEW non_recommendable_offers AS SELECT * FROM non_recommendable_offers_temporary_table;"
        )
    )


def create_non_recommendable_items(engine):
    non_recommendable_offers = pd.DataFrame(
        {"user_id": ["111", "112"], "item_id": ["isbn-1", "isbn-3"]}
    )
    engine.connect().execute(
        text("DROP MATERIALIZED VIEW IF EXISTS non_recommendable_items CASCADE;")
    )

    non_recommendable_offers.to_sql(
        "non_recommendable_items_temporary_table", con=engine, if_exists="replace"
    )
    engine.connect().execute(
        text(
            "CREATE MATERIALIZED VIEW non_recommendable_items AS SELECT * FROM non_recommendable_items_temporary_table;"
        )
    )


def create_recommendable_offers_per_iris_shape(engine):
    recommendable_offers_per_iris_shape = pd.DataFrame(
        {
            "item_id": [
                "isbn-1",
                "isbn-2",
                "movie-3",
                "movie-4",
                "movie-5",
                "product-6",
                "product-7",
                "product-8",
                "product-9",
            ],
            "offer_id": ["1", "2", "3", "4", "5", "6", "7", "8", "9"],
            "iris_id": [
                DEFAULT_IRIS_ID,
                DEFAULT_IRIS_ID,
                DEFAULT_IRIS_ID,
                DEFAULT_IRIS_ID,
                DEFAULT_IRIS_ID,
                DEFAULT_IRIS_ID,
                DEFAULT_IRIS_ID,
                DEFAULT_IRIS_ID,
                DEFAULT_IRIS_ID,
            ],
            "venue_distance_to_iris": [11, 22, 33, 44, 55, 22, 1, 1, 1],
            "is_geolocated": [
                False,
                True,
                False,
                False,
                False,
                True,
                True,
                True,
                False,
            ],
            "venue_latitude": [
                48.87004,
                48.87004,
                48.87004,
                48.87004,
                48.87004,
                48.87004,
                48.830719,
                48.830719,
                48.87004,
            ],
            "venue_longitude": [
                2.3785,
                2.3785,
                2.3785,
                2.3785,
                2.3785,
                2.3785,
                2.331289,
                2.331289,
                2.3785,
            ],
            "unique_id": [1, 2, 3, 4, 5, 6, 7, 8, 9],
        }
    )

    engine.connect().execute(
        text(
            "DROP MATERIALIZED VIEW IF EXISTS recommendable_offers_per_iris_shape_mv CASCADE;"
        )
    )

    recommendable_offers_per_iris_shape.to_sql(
        "recommendable_offers_temporary_table", con=engine, if_exists="replace"
    )
    engine.connect().execute(
        text(
            """
        CREATE MATERIALIZED VIEW recommendable_offers_per_iris_shape_mv AS 
        SELECT 
            ro.item_id,
            ro.offer_id,
            ro.iris_id,
            ro.venue_distance_to_iris,
            ro.is_geolocated,
            ro.venue_latitude,
            ro.venue_longitude,
            ST_MakePoint(ro.venue_longitude, ro.venue_latitude)::geography as venue_geo,
            ro.unique_id        
        FROM recommendable_offers_temporary_table ro
        WITH DATA;
    """
        )
    )


def create_recommendable_offers_raw(engine):
    recommendable_offers_raw = pd.DataFrame(
        {
            "item_id": [
                "isbn-1",
                "isbn-2",
                "isbn-2",
                "movie-3",
                "movie-4",
                "movie-5",
                "product-6",
                "product-7",
                "product-8",
                "product-9",
            ],
            "offer_id": ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10"],
            "product_id": ["1", "2", "2", "3", "4", "5", "6", "7", "8", "9"],
            "category": ["A", "B", "B", "C", "D", "E", "B", "A", "A", "D"],
            "subcategory_id": [
                "EVENEMENT_CINE",
                "EVENEMENT_CINE",
                "EVENEMENT_CINE",
                "EVENEMENT_CINE",
                "EVENEMENT_CINE",
                "SPECTACLE_REPRESENTATION",
                "SPECTACLE_REPRESENTATION",
                "SPECTACLE_REPRESENTATION",
                "EVENEMENT_CINE",
                "LIVRE_PAPIER",
            ],
            "search_group_name": [
                "CINEMA",
                "CINEMA",
                "CINEMA",
                "CINEMA",
                "CINEMA",
                "SPECTACLE",
                "SPECTACLE",
                "SPECTACLE",
                "CINEMA",
                "LIVRE_PAPIER",
            ],
            "offer_type_domain": [
                "MOVIE",
                "MOVIE",
                "MOVIE",
                "MOVIE",
                "MOVIE",
                "SHOW",
                "SHOW",
                "SHOW",
                "MOVIE",
                "BOOK",
            ],
            "offer_type_label": [
                "BOOLYWOOD",
                "BOOLYWOOD",
                "BOOLYWOOD",
                "BOOLYWOOD",
                "BOOLYWOOD",
                "Cirque",
                "Cirque",
                "Cirque",
                "COMEDY",
                "Histoire",
            ],
            "venue_id": ["11", "22", "21", "33", "44", "55", "22", "22", "22", "23"],
            "name": ["a", "b", "b", "c", "d", "e", "f", "g", "h", "i"],
            "is_numerical": [
                False,
                False,
                False,
                True,
                True,
                False,
                False,
                False,
                False,
                False,
            ],
            "is_national": [
                True,
                False,
                False,
                True,
                True,
                True,
                False,
                False,
                False,
                True,
            ],
            "is_geolocated": [
                False,
                True,
                True,
                False,
                False,
                False,
                True,
                True,
                True,
                False,
            ],
            "booking_number": [3, 5, 2, 10, 2, 1, 9, 5, 5, 10],
            "offer_creation_date": [
                "2020-01-01",
                "2020-01-01",
                "2020-01-01",
                "2020-01-01",
                "2020-01-01",
                "2020-01-01",
                "2020-01-01",
                "2020-01-01",
                "2020-01-01",
                "2020-01-01",
            ],
            "stock_beginning_date": [
                "2020-01-01",
                "2020-01-01",
                "2020-01-01",
                "2020-01-01",
                "2020-01-01",
                "2020-01-01",
                "2020-01-01",
                "2020-01-01",
                "2020-01-01",
                "2020-01-01",
            ],
            "stock_price": [10, 20, 20, 20, 30, 30, 30, 30, 30, 10],
            "is_underage_recommendable": [
                False,
                True,
                True,
                True,
                False,
                False,
                False,
                False,
                False,
                True,
            ],
            "venue_latitude": [
                None,
                48.87004,
                48.830719,
                None,
                None,
                None,
                48.87004,
                48.830719,
                48.830719,
                None,
            ],
            "venue_longitude": [
                None,
                2.3785,
                2.331289,
                None,
                None,
                None,
                2.3785,
                2.331289,
                2.331289,
                None,
            ],
            "unique_id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            "default_max_distance": [
                100000,
                100000,
                100000,
                100000,
                100000,
                100000,
                100000,
                100000,
                100000,
                100000,
            ],
        }
    )
    engine.connect().execute(
        text("DROP MATERIALIZED VIEW IF EXISTS recommendable_offers_raw_mv CASCADE;")
    )

    recommendable_offers_raw.to_sql(
        "recommendable_offers_raw", con=engine, if_exists="replace"
    )
    engine.connect().execute(
        text(
            "CREATE MATERIALIZED VIEW recommendable_offers_raw_mv AS SELECT *, ST_MakePoint(venue_longitude, venue_latitude)::geography as venue_geo FROM recommendable_offers_raw WITH DATA;"
        )
    )


def create_recommendable_items_raw(engine):
    recommendable_items_raw = pd.DataFrame(
        {
            "item_id": [
                "isbn-1",
                "isbn-2",
                "movie-3",
                "movie-4",
                "movie-5",
                "product-6",
                "product-7",
                "product-8",
                "product-9",
            ],
            "category": ["A", "B", "C", "D", "E", "B", "A", "A", "D"],
            "subcategory_id": [
                "EVENEMENT_CINE",
                "EVENEMENT_CINE",
                "EVENEMENT_CINE",
                "EVENEMENT_CINE",
                "SPECTACLE_REPRESENTATION",
                "SPECTACLE_REPRESENTATION",
                "SPECTACLE_REPRESENTATION",
                "EVENEMENT_CINE",
                "LIVRE_PAPIER",
            ],
            "search_group_name": [
                "CINEMA",
                "CINEMA",
                "CINEMA",
                "CINEMA",
                "SPECTACLE",
                "SPECTACLE",
                "SPECTACLE",
                "CINEMA",
                "LIVRE_PAPIER",
            ],
            "is_numerical": [
                False,
                False,
                True,
                True,
                False,
                False,
                False,
                False,
                False,
            ],
            "is_national": [True, False, True, True, True, False, False, False, True],
            "is_geolocated": [
                False,
                True,
                False,
                False,
                False,
                True,
                True,
                True,
                False,
            ],
            "offer_type_domain": [
                "MOVIE",
                "MOVIE",
                "MOVIE",
                "MOVIE",
                "SHOW",
                "SHOW",
                "SHOW",
                "MOVIE",
                "BOOK",
            ],
            "offer_type_label": [
                "BOOLYWOOD",
                "BOOLYWOOD",
                "BOOLYWOOD",
                "BOOLYWOOD",
                "Cirque",
                "Cirque",
                "Cirque",
                "COMEDY",
                "Histoire",
            ],
            "booking_number": [3, 5, 10, 2, 1, 9, 5, 5, 10],
            "is_underage_recommendable": [
                False,
                True,
                True,
                False,
                False,
                False,
                False,
                False,
                True,
            ],
            "offer_creation_date": [
                "2020-01-01",
                "2020-01-01",
                "2020-01-01",
                "2020-01-01",
                "2020-01-01",
                "2020-01-01",
                "2020-01-01",
                "2020-01-01",
                "2020-01-01",
            ],
            "stock_beginning_date": [
                "2020-01-01",
                "2020-01-01",
                "2020-01-01",
                "2020-01-01",
                "2020-01-01",
                "2020-01-01",
                "2020-01-01",
                "2020-01-01",
                "2020-01-01",
            ],
            "stock_price": [10, 20, 20, 30, 30, 30, 30, 30, 10],
        }
    )
    engine.connect().execute(
        text("DROP MATERIALIZED VIEW IF EXISTS recommendable_items_raw_mv CASCADE;")
    )

    recommendable_items_raw.to_sql(
        "recommendable_items_raw", con=engine, if_exists="replace"
    )
    engine.connect().execute(
        text(
            "CREATE MATERIALIZED VIEW recommendable_items_raw_mv AS SELECT * FROM recommendable_items_raw WITH DATA;"
        )
    )


def create_enriched_user(engine):
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
    engine.connect().execute(
        text(
            "CREATE MATERIALIZED VIEW enriched_user_mv AS SELECT * FROM enriched_user;"
        )
    )


def create_qpi_answers(engine):
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


def create_past_recommended_offers(engine):
    past_recommended_offers = pd.DataFrame(
        {
            "userid": [1],
            "offerid": [1],
            "date": [datetime.now(pytz.utc)],
            "reco_origin": "algo",
        }
    )
    past_recommended_offers.to_sql(
        "past_recommended_offers", con=engine, if_exists="replace"
    )


def create_iris_france(engine, connection):
    iris_france = pd.read_csv("./src/tests/iris_france_tests.csv")
    iris_france.to_sql("iris_france", con=engine, if_exists="replace", index=False)
    sql = """ALTER TABLE public.iris_france
            ALTER COLUMN shape TYPE Geometry(GEOMETRY, 4326)
            USING ST_SetSRID(shape::Geometry, 4326);
        """

    engine.connect().execute(text(sql))


@pytest.fixture
def setup_database(app_config: Dict[str, Any]) -> Any:
    engine = create_engine(
        f"postgresql+psycopg2://postgres:postgres@127.0.0.1:{DATA_GCP_TEST_POSTGRES_PORT}/{DB_NAME}"
    )
    connection = engine.connect().execution_options(autocommit=True)

    create_recommendable_offers_per_iris_shape(engine)

    create_recommendable_offers_raw(engine)
    create_recommendable_items_raw(engine)

    create_non_recommendable_offers(engine)
    create_non_recommendable_items(engine)

    create_enriched_user(engine)
    create_qpi_answers(engine)
    create_past_recommended_offers(engine)
    create_iris_france(engine, connection)

    yield connection
    try:
        engine.connect().execute(
            "DROP MATERIALIZED VIEW IF EXISTS recommendable_offers_per_iris_shape_mv CASCADE;"
        )

        engine.connect().execute(
            "DROP MATERIALIZED VIEW IF EXISTS recommendable_offers_raw_mv CASCADE;"
        )

        engine.connect().execute(
            "DROP MATERIALIZED VIEW IF EXISTS recommendable_items_raw_mv CASCADE;"
        )

        engine.connect().execute(
            "DROP MATERIALIZED VIEW IF EXISTS non_recommendable_offers CASCADE;"
        )
        engine.connect().execute(
            "DROP MATERIALIZED VIEW IF EXISTS non_recommendable_items CASCADE;"
        )

        engine.connect().execute(
            "DROP TABLE IF EXISTS recommendable_offers_temporary_table CASCADE;"
        )
        engine.connect().execute(
            "DROP TABLE IF EXISTS recommendable_offers_raw CASCADE;"
        )
        engine.connect().execute(
            "DROP TABLE IF EXISTS recommendable_items_raw CASCADE;"
        )

        engine.connect().execute(
            "DROP TABLE IF EXISTS non_recommendable_offers_temporary_table CASCADE;"
        )
        engine.connect().execute("DROP TABLE IF EXISTS enriched_user CASCADE;")
        engine.connect().execute(
            "DROP MATERIALIZED VIEW IF EXISTS enriched_user_mv CASCADE;"
        )
        engine.connect().execute(
            "DROP TABLE IF EXISTS past_recommended_offers CASCADE ;"
        )
        engine.connect().execute("DROP TABLE IF EXISTS iris_france CASCADE;")
    except:
        pass
    finally:
        connection.close()
