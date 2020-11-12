import pandas as pd
import pytest
import psycopg2


TEST_DATA = {
    "booking": [
        (True, 69434, '2019-09-28 15:05:37.920197', 42001473, 36326, 1, "SEU4ZU", 25173, 38.00, False, True,
         "2019-09-28 15:06:11.961879", None, None),
        (True, 69435, '2019-09-28 15:05:37.920197', 42001473, 36326, 1, "SEU4ZU", 25173, 38.00, False, True,
         "2019-09-28 15:06:11.961879", None, None)
    ],
    "iris_venues": [
        (14552684, 31, 7079),
        (14552685, 54, 7079),
        (14552686, 57, 7079)
    ],
    "mediation": [
        (
            1, 64, "2018-05-15 12:00:00", 37, "2018-05-15 17:35:48", None, 4,
            1017696,  # offerId
            None,
            True,  # isActive
            "{}"
        )
    ],
    "offer": [
        (
            None, "2020-02-16 09:32:08",
            1017696,  # offerId
            "2020-02-16 09:32:08", 2818251,
            2833,  # venueId
            None, "foliesdencre@hotmail.fr",
            True,  # isActive
            "ThingType.LIVRE_EDITION",  # type not in [ThingType.ACTIVATION, EventType.ACTIVATION]
            "sociologie", "Livre sociologie", None, None, None, None, "{}", None, False,
            '{"author": "Pierre-Andr\u00e9 Corpron ", "isbn": "9782749539010"}', False, "{}", None
        )
    ],
    "offerer": [
        (
            0, None, None, "2018-07-10 16:50:33",
            2861,  # offererId
            "BIBLIOTHEQUE NATIONALE DE FRANCE",
            "11 Quai François Mauriac 75013 Paris", None, 75013, "PARIS 13", 180046252,
            True,  # isActive
            None,  # validationToken
            "2018-07-10 16:50:33", "{}"
        )
    ],
    "venue": [
        (
            0, None, None, "2019-09-23 09:43:39",
            2833,  # venueId
            "YOUSCRIBE", "13 RUE DU MAIL", 48.86673, 2.34225,
            None, 75, 75002, "PARIS 2E ARRONDISSEMENT", 52205665400026,
            2861,  # managingOffererId
            "juanpc@youscribe.com",
            False, None,
            None,  # validationToken
            "Société YouScribe", "{}", 13, None, "2019-09-23 09:43:39"
        )
    ],
    "stock": [
        (
            None, "2021-07-21 10:01:34", 2486130, "2021-07-21 10:01:34", 19.95,
            1000,  # quantity (+ no booking)
            None,  # bookingLimitDatetime
            None,
            1017696,  # offerId
            False,  # isSoftDeleted
            None,  # beginningDatetime
            "2021-07-21 10:01:34", "{}", None
        )
    ]
}


def create_and_fill_tables(cursor, data):
    tables = pd.read_csv('tests/tables.csv')

    for table in data:
        table_data = tables.loc[lambda df: df.table_name == table]
        columns = ', '.join(
            [
                column_name for column_name in list(table_data.column_name.values)
            ]
        )
        typed_columns = ', '.join(
            [
                f"{column_name} {data_type}"
                for column_name, data_type in zip(
                    list(table_data.column_name.values), list(table_data.data_type.values)
                )
            ]
        )
        values = ', '.join(['%s'] * table_data.shape[0])
        cursor.execute(f"DROP TABLE IF EXISTS public.{table}")
        cursor.execute(f"CREATE TABLE IF NOT EXISTS public.{table} ({typed_columns})")

        for row in data[table]:
            cursor.execute(
                f'INSERT INTO public.{table} '
                f'({columns}) '
                f'VALUES ({values})', row
            )


def create_recommendable_offers(cursor):
    with open('cloudsql/scripts/create_recommendable_offers.sql', 'r') as f:
        sql = f.read().replace('"', '')
        sql = ' '.join(sql.split())

    cursor.execute(sql)


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

    create_and_fill_tables(cursor, TEST_DATA)
    create_recommendable_offers(cursor)

    connection.commit()

    yield cursor


def test_data_ingestion(setup_database):
    # Test that test data is properly stored in database
    cursor = setup_database
    for table in TEST_DATA:
        cursor.execute(f'SELECT * FROM public.{table}')
        assert len(cursor.fetchall()) == len(TEST_DATA[table])
    cursor.close()


def test_recommendable_offer(setup_database):
    cursor = setup_database
    cursor.execute('SELECT * FROM recommendable_offers where id = 1017696')
    assert len(cursor.fetchall()) == 1
    cursor.close()


def test_non_active_offer_filtered(setup_database):
    cursor = setup_database
    cursor.execute("UPDATE public.offer SET isActive = False where id = 1017696")
    cursor.execute('REFRESH MATERIALIZED VIEW recommendable_offers')
    cursor.execute('SELECT * FROM recommendable_offers where id = 1017696')
    assert len(cursor.fetchall()) == 0
    cursor.close()
