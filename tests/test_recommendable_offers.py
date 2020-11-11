import pandas as pd
import pytest
import psycopg2


@pytest.fixture
def setup_database():
    """ Fixture to set up the in-memory database with test data """
    connection = psycopg2.connect(
        user="postgres",
        password="postgres",
        host="127.0.0.1",
        port="5432",
        database="postgres"
    )
    cursor = connection.cursor()

    tables = pd.read_csv('tests/tables.csv')

    for table in list(set(tables.table_name.values)):
        table_data = tables.loc[lambda df: df.table_name == table]
        columns = ', '.join(
            [
                f"{column_name} {data_type}"
                for column_name, data_type in zip(
                    list(table_data.column_name.values), list(table_data.data_type.values)
                )
            ]
        )
        cursor.execute(f"DROP TABLE IF EXISTS public.{table}")
        cursor.execute(f"CREATE TABLE IF NOT EXISTS public.{table} ({columns})")

    yield cursor


@pytest.fixture
def setup_test_data1(setup_database):
    cursor = setup_database
    sample_data = [
        (True, 69434, '2019-09-28 15:05:37.920197', 42001473, 36326, 1, "SEU4ZU", 25173, 38.00, False, True,
         "2019-09-28 15:06:11.961879"),
        (True, 69435, '2019-09-28 15:05:37.920197', 42001473, 36326, 1, "SEU4ZU", 25173, 38.00, False, True,
         "2019-09-28 15:06:11.961879")
    ]
    for data in sample_data:
        cursor.execute(f'INSERT INTO booking VALUES {str(data)}')
    yield cursor


def test_with_sample_data2(setup_test_data1):
    # Test to make sure that there are 3 items in the database
    cursor = setup_test_data1
    cursor.execute('SELECT * FROM public.booking')
    assert len(cursor.fetchall()) == 2
