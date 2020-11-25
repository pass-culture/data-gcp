import os
from datetime import datetime, timedelta

import pandas as pd
import psycopg2
import pytest

TEST_DATABASE_CONFIG = {
    "user": "postgres",
    "password": "postgres",
    "host": "127.0.0.1",
    "port": os.getenv("DATA_GCP_TEST_POSTGRES_PORT"),
    "database": "postgres",
}

TEST_DATA = {
    "booking": [
        (
            True,  # isActive
            69434,
            "2019-09-28 15:05:37.920197",
            42001473,
            2486130,  # stockId
            0,  # quantity
            "SEU4ZU",
            25173,  # userId
            38.00,  # amount
            False,  # isCancelled
            True,  # isUsed
            "2019-09-28 15:06:11.961879",
            None,
            None,
        )
    ],
    "iris_venues": [(14552684, 31, 7079), (14552685, 54, 7079), (14552686, 57, 7079)],
    "mediation": [
        (
            1,
            64,
            "2018-05-15 12:00:00",
            37,
            "2018-05-15 17:35:48",
            None,
            4,
            1017696,  # offerId
            None,
            True,  # isActive
            "{}",
        )
    ],
    "offer": [
        (
            None,
            "2020-02-16 09:32:08",
            1017696,  # offerId
            "2020-02-16 09:32:08",
            2818251,
            2833,  # venueId
            None,
            "foliesdencre@hotmail.fr",
            True,  # isActive
            "ThingType.LIVRE_EDITION",  # type not in [ThingType.ACTIVATION, EventType.ACTIVATION]
            "sociologie",
            "Livre sociologie",
            None,
            None,
            None,
            None,
            "{}",
            None,
            False,
            '{"author": "Pierre-Andr\u00e9 Corpron ", "isbn": "9782749539010"}',
            False,
            "{}",
            None,
        )
    ],
    "offerer": [
        (
            0,
            None,
            None,
            "2018-07-10 16:50:33",
            2861,  # offererId
            "BIBLIOTHEQUE NATIONALE DE FRANCE",
            "11 Quai François Mauriac 75013 Paris",
            None,
            75013,
            "PARIS 13",
            180046252,
            True,  # isActive
            None,  # validationToken
            "2018-07-10 16:50:33",
            "{}",
        )
    ],
    "venue": [
        (
            0,
            None,
            None,
            "2019-09-23 09:43:39",
            2833,  # venueId
            "YOUSCRIBE",
            "13 RUE DU MAIL",
            48.86673,
            2.34225,
            None,
            75,
            75002,
            "PARIS 2E ARRONDISSEMENT",
            52205665400026,
            2861,  # managingOffererId
            "juanpc@youscribe.com",
            False,
            None,
            None,  # validationToken
            "Société YouScribe",
            "{}",
            13,
            None,
            "2019-09-23 09:43:39",
        )
    ],
    "stock": [
        (
            None,
            "2021-07-21 10:01:34",
            2486130,  # stockId
            "2021-07-21 10:01:34",
            19.95,
            1,  # quantity (+ no booking)
            None,  # bookingLimitDatetime
            None,
            1017696,  # offerId
            False,  # isSoftDeleted
            None,  # beginningDatetime
            "2021-07-21 10:01:34",
            "{}",
            None,
        )
    ],
}

yesterday = (datetime.now() + timedelta(days=-1)).strftime("%Y-%m-%d %H:%M:%S.%f")
tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S.%f")


def create_and_fill_tables(cursor, data):
    tables = pd.read_csv("tests/tables.csv")

    for table in data:
        table_data = tables.loc[lambda df: df.table_name == table]
        columns = ", ".join(
            [column_name for column_name in list(table_data.column_name.values)]
        )
        typed_columns = ", ".join(
            [
                f"{column_name} {data_type}"
                for column_name, data_type in zip(
                    list(table_data.column_name.values),
                    list(table_data.data_type.values),
                )
            ]
        )
        value_placeholders = ", ".join(["%s"] * table_data.shape[0])
        cursor.execute(f"DROP TABLE IF EXISTS public.{table}")
        cursor.execute(f"CREATE TABLE IF NOT EXISTS public.{table} ({typed_columns})")

        for row in data[table]:
            cursor.execute(
                f"INSERT INTO public.{table} "
                f"({columns}) "
                f"VALUES ({value_placeholders})",
                row,
            )


def run_sql_script(cursor, script_path):
    with open(script_path, "r") as f:
        sql = f.read().replace('"', "")
    cursor.execute(sql)


@pytest.fixture
def setup_database():
    """
    Fixture to set up the test postgres database with test data.
    """
    connection = psycopg2.connect(**TEST_DATABASE_CONFIG)
    cursor = connection.cursor()

    create_and_fill_tables(cursor, TEST_DATA)
    run_sql_script(cursor, "recommendation/db/scripts/create_recommendable_offers.sql")
    run_sql_script(
        cursor, "recommendation/db/scripts/create_non_recommendable_offers.sql"
    )
    connection.commit()

    return connection, cursor


def test_data_ingestion(setup_database):
    """
    Test that test data is loaded in test postgres.
    """
    connection, cursor = setup_database
    for table in TEST_DATA:
        cursor.execute(f"SELECT * FROM public.{table}")
        assert len(cursor.fetchall()) == len(TEST_DATA[table])
    cursor.close()
    connection.close()


def test_recommendable_offer_non_filtered(setup_database):
    """
    Test that an offer respecting the criteria is not filtered.
    """
    connection, cursor = setup_database
    cursor.execute("SELECT * FROM recommendable_offers where id = 1017696")
    assert len(cursor.fetchall()) == 1
    cursor.close()
    connection.close()


@pytest.mark.parametrize(
    ["name", "query", "recommendable"],
    [
        (
            "non_active_offer",
            "UPDATE public.offer SET isActive = False where id = 1017696",
            False,
        ),
        (
            "offer_with_thing_type_activation_type",
            "UPDATE public.offer SET type = 'ThingType.ACTIVATION' where id = 1017696",
            False,
        ),
        (
            "offer_with_event_type_activation_type",
            "UPDATE public.offer SET type = 'EventType.ACTIVATION' where id = 1017696",
            False,
        ),
        (
            "offer_without_mediation",
            "DELETE FROM public.mediation where offerId = 1017696",
            False,
        ),
        (
            "offer_with_inactive_mediation",
            "UPDATE public.mediation SET isActive = false where offerId = 1017696",
            False,
        ),
        (
            "offer_without_stock",
            "DELETE FROM public.stock where offerId = 1017696",
            False,
        ),
        (
            "offer_with_stock_at_0",
            "UPDATE public.stock SET quantity = 0 where offerId = 1017696",
            False,
        ),
        (
            "offer_with_more_bookings_than_stock",
            "UPDATE public.booking SET quantity = 1 where stockId = 2486130",
            False,
        ),
        (
            "offer_with_soft_deleted_stock",
            "UPDATE public.stock SET isSoftDeleted = true where offerId = 1017696",
            False,
        ),
        (
            "offer_with_passed_beginning_date_time",
            f"UPDATE public.stock SET beginningDatetime = '{yesterday}' where offerId = 1017696",
            False,
        ),
        (
            "offer_with_passed_limit_booking_date_time",
            f"UPDATE public.stock SET bookingLimitDatetime = '{yesterday}' where offerId = 1017696",
            False,
        ),
        (
            "offer_with_venue_validation_token_not_null",
            "UPDATE public.venue SET validationToken='' where managingOffererId = 2861",
            False,
        ),
        (
            "offer_with_offerer_validation_token_not_null",
            "UPDATE public.offerer SET validationToken='' where id = 2861",
            False,
        ),
        (
            "offer_with_inactive_offerer",
            "UPDATE public.offerer SET isActive = false where id = 2861",
            False,
        ),
        (
            "offer_with_more_canceled_bookings_than_stocked",
            "UPDATE public.booking SET quantity = 1, isCancelled = true where stockId = 2486130",
            True,
        ),
        (
            "offer_with_future_beginning_date_time",
            f"UPDATE public.stock SET beginningDatetime = '{tomorrow}' where offerId = 1017696",
            True,
        ),
        (
            "offer_with_future_limit_booking_date_time",
            f"UPDATE public.stock SET bookingLimitDatetime = '{tomorrow}' where offerId = 1017696",
            True,
        ),
    ],
)
def test_updated_offer_in_recommendable_offers(
    setup_database, name, query, recommendable
):
    """
    Test that an update on the (initially recommendable) offer data
    has the expected impact on its recommendable status and its presence in
    the recommendable_offers materialized view.
    """
    connection, cursor = setup_database
    cursor.execute(query)
    cursor.execute("REFRESH MATERIALIZED VIEW recommendable_offers")
    cursor.execute("SELECT * FROM recommendable_offers where id = 1017696")
    result = len(cursor.fetchall())

    cursor.close()
    connection.close()

    assert result == (1 if recommendable else 0)


@pytest.mark.parametrize(
    ["name", "query", "recommendable"],
    [
        (
            "recommendable_offer",
            "SELECT * from non_recommendable_offers limit 1;",
            True,
        ),
        (
            "offer_already_booked_by_user",
            """
                UPDATE public.booking SET userId = 1017696, quantity = 1, isActive = true, isCancelled = false
                where stockId = 2486130
            """,
            False,
        ),
        (
            "offer_already_booked_by_user_but_canceled",
            """
                UPDATE public.booking SET userId = 1017696, quantity = 1, isActive = true, isCancelled = true
                where stockId = 2486130
            """,
            True,
        ),
        (
            "offer_already_booked_by_user_but_inactive",
            """
                UPDATE public.booking SET userId = 1017696, quantity = 1, isActive = false, isCancelled = false
                where stockId = 2486130
            """,
            True,
        ),
    ],
)
def test_updated_offer_in_non_recommendable_offers(
    setup_database, name, query, recommendable
):
    """
    Test that an update on the (initially recommendable) offer data
    has the expected impact on its recommendable status and its presence in
    the non_recommendable_offers materialized view.
    """
    connection, cursor = setup_database
    cursor.execute(query)
    cursor.execute("REFRESH MATERIALIZED VIEW non_recommendable_offers")
    cursor.execute("SELECT * FROM non_recommendable_offers where user_id = 1017696")
    result = len(cursor.fetchall())

    cursor.close()
    connection.close()

    assert result == (0 if recommendable else 1)
