import json
import os
from datetime import datetime, timedelta

import pandas as pd
import pytest
from sqlalchemy import create_engine

DATA_GCP_TEST_POSTGRES_PORT = os.getenv("DATA_GCP_TEST_POSTGRES_PORT")
yesterday = (datetime.now() + timedelta(days=-1)).strftime("%Y-%m-%d %H:%M:%S.%f")
tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S.%f")


def create_and_fill_tables(connection):
    tables = pd.read_csv("tests/tables.csv")

    for table in set(tables["table_name"].values):
        table_data = tables[tables.table_name == table]
        schema = {
            column_name: data_type
            for column_name, data_type in zip(
                list(table_data.column_name.values),
                list(table_data.data_type.values),
            )
            if "[]" not in data_type
        }
        typed_columns = ", ".join(
            [f'"{column_name}" {schema[column_name]}' for column_name in schema]
        )
        try:
            connection.execute(f"DROP TABLE IF EXISTS public.{table} CASCADE;")
        except:
            pass
        connection.execute(
            f"CREATE TABLE IF NOT EXISTS public.{table} ({typed_columns})"
        )
        dataframe = pd.read_csv(f"tests/tables/{table}.csv", sep=",")
        dataframe.to_sql(table, con=connection, if_exists="append", index=False)


def run_sql_script(connection, script_path):
    with open(script_path, "r") as f:
        sql = f.read()
    connection.execute(sql)


@pytest.fixture
def setup_database():
    """
    Fixture to set up the test postgres database with test data.
    """
    engine = create_engine(
        f"postgresql+psycopg2://postgres:postgres@127.0.0.1:{DATA_GCP_TEST_POSTGRES_PORT}/postgres"
    )
    connection = engine.connect().execution_options(autocommit=True)

    create_and_fill_tables(connection)
    run_sql_script(connection, "scripts/create_recommendable_offers.sql")
    run_sql_script(connection, "scripts/create_non_recommendable_offers.sql")

    yield connection

    connection.close()


def test_data():
    with open("tests/tables/data.json", "r") as f:
        checks = json.loads(f.read())
    tables = pd.read_csv("tests/tables.csv")
    for table in set(tables["table_name"].values):
        df = pd.read_csv(f"tests/tables/{table}.csv", sep=",")
        for column in checks[table]:
            if checks[table][column] == "nan":
                assert pd.isnull(df[column].values[0])
            else:
                assert checks[table][column] == df[column].values[0]


def test_data_ingestion(setup_database):
    """
    Test that test data is loaded in test postgres.
    """
    connection = setup_database
    tables = pd.read_csv("tests/tables.csv")
    for table in set(tables["table_name"].values):
        query_result = connection.execute(f"SELECT * FROM public.{table}").fetchall()
        assert len(query_result) > 0


def test_recommendable_offer_non_filtered(setup_database):
    """
    Test that an offer respecting the criteria is not filtered.
    """
    connection = setup_database
    query_result = connection.execute(
        "SELECT * FROM recommendable_offers where offer_id = '138717'"
    ).fetchall()
    assert len(query_result) == 1


@pytest.mark.parametrize(
    ["name", "query", "recommendable"],
    [
        (
            "non_active_offer",
            """UPDATE public.offer SET offer_is_active = False where offer_id = '138717'""",
            False,
        ),
        (
            "offer_with_thing_type_activation_type",
            """UPDATE public.offer SET "offer_subcategoryId" = 'ACTIVATION_THING' where offer_id = '138717'""",
            False,
        ),
        (
            "offer_with_event_type_activation_type",
            """UPDATE public.offer SET "offer_subcategoryId" = 'ACTIVATION_EVENT' where offer_id = '138717'""",
            False,
        ),
        (
            "offer_without_mediation",
            """DELETE FROM public.mediation where "offerId" = '138717'""",
            False,
        ),
        (
            "offer_with_inactive_mediation",
            """UPDATE public.mediation SET "isActive" = false where "offerId" = '138717'""",
            False,
        ),
        (
            "offer_without_stock",
            """DELETE FROM public.stock where offer_id = '138717'""",
            False,
        ),
        (
            "offer_with_stock_at_0",
            """UPDATE public.stock SET stock_quantity = 0 where offer_id = '138717'""",
            False,
        ),
        (
            "offer_with_more_bookings_than_stock",
            """UPDATE public.booking SET booking_quantity = 1 where booking_id = '44053'""",
            False,
        ),
        (
            "offer_with_soft_deleted_stock",
            """UPDATE public.stock SET stock_is_soft_deleted = true where offer_id = '138717'""",
            False,
        ),
        (
            "offer_with_passed_beginning_date_time",
            f"""UPDATE public.stock SET stock_beginning_date = '{yesterday}' where offer_id = '138717'""",
            False,
        ),
        (
            "offer_with_passed_limit_booking_date_time",
            f"""UPDATE public.stock SET stock_booking_limit_date = '{yesterday}' where offer_id = '138717'""",
            False,
        ),
        (
            "offer_with_inactive_offerer",
            """UPDATE public.offerer SET offerer_is_active = false where offerer_id = '1073'""",
            False,
        ),
        (
            "offer_with_more_canceled_bookings_than_stocked",
            """UPDATE public.booking SET booking_quantity = 1, booking_is_cancelled = true where stock_id = '141098'""",
            True,
        ),
        (
            "offer_with_future_beginning_date_time",
            f"""UPDATE public.stock SET stock_beginning_date = '{tomorrow}' where offer_id = '138717'""",
            True,
        ),
        (
            "offer_with_future_limit_booking_date_time",
            f"""UPDATE public.stock SET stock_booking_limit_date = '{tomorrow}' where offer_id = '138717'""",
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
    connection = setup_database
    connection.execute(query)
    connection.execute("REFRESH MATERIALIZED VIEW recommendable_offers")
    query_result = connection.execute(
        "SELECT * FROM recommendable_offers where offer_id = '138717'"
    ).fetchall()

    assert len(query_result) == (1 if recommendable else 0)


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
                UPDATE public.booking SET user_id = '138717', booking_quantity = 1, booking_is_cancelled = false
                where stock_id = '141098'
            """,
            False,
        ),
        (
            "offer_already_booked_by_user_but_canceled",
            """
                UPDATE public.booking SET user_id = '138717', booking_quantity = 1, booking_is_cancelled = true
                where stock_id = '141098'
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
    connection = setup_database
    connection.execute(query)
    connection.execute("REFRESH MATERIALIZED VIEW non_recommendable_offers")
    query_result = connection.execute(
        "SELECT * FROM non_recommendable_offers where user_id = '138717'"
    ).fetchall()

    assert len(query_result) == (0 if recommendable else 1)
