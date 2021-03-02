import pytest
from analytics.tests.config import GCP_PROJECT, TEST_DATASET, TEST_TABLE_PREFIX
from analytics.tests.data import (
    ENRICHED_BOOKED_CATEGORIES_DATA_EXPECTED,
    ENRICHED_BOOKED_CATEGORIES_DATA_INPUT,
    ENRICHED_BOOKING_DATA_EXPECTED,
    ENRICHED_BOOKING_DATA_INPUT,
    ENRICHED_OFFER_DATA_EXPECTED,
    ENRICHED_OFFER_DATA_INPUT,
    ENRICHED_OFFERER_DATA_EXPECTED,
    ENRICHED_OFFERER_DATA_INPUT,
    ENRICHED_STOCK_DATA_EXPECTED,
    ENRICHED_STOCK_DATA_INPUT,
    ENRICHED_USER_DATA_EXPECTED,
    ENRICHED_USER_DATA_INPUT,
    ENRICHED_VENUE_DATA_EXPECTED,
    ENRICHED_VENUE_DATA_INPUT,
    TEST_TABLE_PREFIX,
)
from analytics.tests.utils import (
    create_data,
    create_dataset,
    drop_dataset,
    drop_table,
    get_table_columns,
    retrieve_data,
    run_query,
)
from dependencies.data_analytics.enriched_data.booked_categories import (
    define_enriched_booked_categories_data_full_query,
)
from dependencies.data_analytics.enriched_data.booking import (
    define_enriched_booking_data_full_query,
)
from dependencies.data_analytics.enriched_data.offer import (
    define_enriched_offer_data_full_query,
)
from dependencies.data_analytics.enriched_data.offerer import (
    define_enriched_offerer_data_full_query,
)
from dependencies.data_analytics.enriched_data.stock import (
    define_enriched_stock_data_full_query,
)
from dependencies.data_analytics.enriched_data.user import (
    define_enriched_user_data_full_query,
)
from dependencies.data_analytics.enriched_data.venue import (
    define_enriched_venue_data_full_query,
)
from google.cloud import bigquery
from set_env import set_env_vars


@pytest.fixture(scope="module", autouse=True)
def prepare_bigquery():
    set_env_vars()
    pytest.bq_client = bigquery.Client()
    drop_dataset(client=pytest.bq_client, dataset=TEST_DATASET)
    create_dataset(client=pytest.bq_client, dataset=TEST_DATASET)
    yield
    drop_dataset(client=pytest.bq_client, dataset=TEST_DATASET)


@pytest.fixture()
def flush_dataset():
    yield
    dataset_id = f"{GCP_PROJECT}.{TEST_DATASET}"
    tables = pytest.bq_client.list_tables(dataset_id)
    for table in tables:
        drop_table(client=pytest.bq_client, dataset=TEST_DATASET, table=table.table_id)


@pytest.mark.parametrize(
    ["table_name", "query", "input_data", "expected", "sorting_key"],
    [
        (
            "enriched_offer_data",
            define_enriched_offer_data_full_query(
                dataset=TEST_DATASET, table_prefix=TEST_TABLE_PREFIX
            ),
            ENRICHED_OFFER_DATA_INPUT,
            ENRICHED_OFFER_DATA_EXPECTED,
            "offer_id",
        ),
        (
            "enriched_stock_data",
            define_enriched_stock_data_full_query(
                dataset=TEST_DATASET, table_prefix=TEST_TABLE_PREFIX
            ),
            ENRICHED_STOCK_DATA_INPUT,
            ENRICHED_STOCK_DATA_EXPECTED,
            "stock_id",
        ),
    ],
)
def test_create_queries(
    flush_dataset, table_name, query, input_data, expected, sorting_key
):
    create_data(
        client=pytest.bq_client,
        dataset=TEST_DATASET,
        data=input_data,
        table_prefix=TEST_TABLE_PREFIX,
    )
    run_query(client=pytest.bq_client, query=query)
    output = retrieve_data(
        client=pytest.bq_client, dataset=TEST_DATASET, table=table_name, table_prefix=""
    )
    assert sorted(output, key=lambda d: d[sorting_key]) == sorted(
        expected, key=lambda d: d[sorting_key]
    )


@pytest.mark.parametrize(
    ["table_name", "query", "input_data", "expected"],
    [
        (
            "enriched_user_data",
            define_enriched_user_data_full_query(
                dataset=TEST_DATASET, table_prefix=TEST_TABLE_PREFIX
            ),
            ENRICHED_USER_DATA_INPUT,
            ENRICHED_USER_DATA_EXPECTED,
        ),
        (
            "enriched_venue_data",
            define_enriched_venue_data_full_query(
                dataset=TEST_DATASET, table_prefix=TEST_TABLE_PREFIX
            ),
            ENRICHED_VENUE_DATA_INPUT,
            ENRICHED_VENUE_DATA_EXPECTED,
        ),
        (
            "enriched_offerer_data",
            define_enriched_offerer_data_full_query(
                dataset=TEST_DATASET, table_prefix=TEST_TABLE_PREFIX
            ),
            ENRICHED_OFFERER_DATA_INPUT,
            ENRICHED_OFFERER_DATA_EXPECTED,
        ),
        (
            "enriched_booking_data",
            define_enriched_booking_data_full_query(
                dataset=TEST_DATASET, table_prefix=TEST_TABLE_PREFIX
            ),
            ENRICHED_BOOKING_DATA_INPUT,
            ENRICHED_BOOKING_DATA_EXPECTED,
        ),
    ],
)
def test_create_queries_empty(flush_dataset, table_name, query, input_data, expected):
    create_data(
        client=pytest.bq_client,
        dataset=TEST_DATASET,
        data=input_data,
        table_prefix=TEST_TABLE_PREFIX,
    )
    run_query(client=pytest.bq_client, query=query)
    output = get_table_columns(
        client=pytest.bq_client, dataset=TEST_DATASET, table=table_name, table_prefix=""
    )
    assert output == expected


@pytest.mark.parametrize(
    ["table_name", "query", "input_data", "expected"],
    [
        (
            "enriched_booked_categories_data",
            define_enriched_booked_categories_data_full_query(
                dataset=TEST_DATASET, table_prefix=TEST_TABLE_PREFIX
            ),
            ENRICHED_BOOKED_CATEGORIES_DATA_INPUT,
            ENRICHED_BOOKED_CATEGORIES_DATA_EXPECTED,
        )
    ],
)
def test_enriched_booked_categories_data_returns_expected_columns(
    flush_dataset, table_name, query, input_data, expected
):
    # Given
    create_data(
        client=pytest.bq_client,
        dataset=TEST_DATASET,
        data=input_data,
        table_prefix=TEST_TABLE_PREFIX,
    )

    # When
    run_query(client=pytest.bq_client, query=query)
    output = get_table_columns(
        client=pytest.bq_client,
        dataset=TEST_DATASET,
        table=table_name,
        table_prefix="",
    )

    # Then
    assert output == expected


@pytest.mark.parametrize(
    ["table_name", "query", "input_data", "expected"],
    [
        (
            "enriched_booked_categories_data",
            define_enriched_booked_categories_data_full_query(
                dataset=TEST_DATASET, table_prefix=TEST_TABLE_PREFIX
            ),
            {
                "booking": [
                    {
                        "user_id": "1",
                        "stock_id": "1",
                        "booking_id": "4",
                    }
                ],
                "stock": [{"stock_id": "1", "offer_id": "1"}],
                "offer": [{"offer_id": "1", "offer_type": "ThingType.AUDIOVISUEL"}],
                "venue": [],
            },
            [
                {
                    "audiovisuel": True,
                    "cinema": False,
                    "instrument": False,
                    "jeux_video": False,
                    "livre_numerique": False,
                    "livre_papier": False,
                    "musee_patrimoine": False,
                    "musique_cd_vynils": False,
                    "musique_live": False,
                    "musique_numerique": False,
                    "pratique_artistique": False,
                    "spectacle_vivant": False,
                    "user_id": "1",
                },
            ],
        )
    ],
)
def test_enriched_booked_categories_data_returns_expected_audiovisuel(
    flush_dataset, table_name, query, input_data, expected
):
    # Given
    create_data(
        client=pytest.bq_client,
        dataset=TEST_DATASET,
        data=input_data,
        table_prefix=TEST_TABLE_PREFIX,
    )

    # When
    run_query(client=pytest.bq_client, query=query)
    output = retrieve_data(
        client=pytest.bq_client, dataset=TEST_DATASET, table=table_name, table_prefix=""
    )

    # Then
    assert output == expected


@pytest.mark.parametrize(
    ["table_name", "query", "input_data", "expected"],
    [
        (
            "enriched_booked_categories_data",
            define_enriched_booked_categories_data_full_query(
                dataset=TEST_DATASET, table_prefix=TEST_TABLE_PREFIX
            ),
            {
                "booking": [
                    {"user_id": "1", "stock_id": "1", "booking_id": "1"},
                    {"user_id": "3", "stock_id": "2", "booking_id": "2"},
                    {"user_id": "6", "stock_id": "3", "booking_id": "3"},
                ],
                "stock": [
                    {"stock_id": "1", "offer_id": "1"},
                    {"stock_id": "2", "offer_id": "2"},
                    {"stock_id": "3", "offer_id": "3"},
                ],
                "offer": [
                    {"offer_id": "1", "offer_type": "EventType.CINEMA"},
                    {"offer_id": "2", "offer_type": "ThingType.CINEMA_ABO"},
                    {"offer_id": "3", "offer_type": "ThingType.CINEMA_CARD"},
                ],
                "venue": [],
            },
            [
                {
                    "audiovisuel": False,
                    "cinema": True,
                    "instrument": False,
                    "jeux_video": False,
                    "livre_numerique": False,
                    "livre_papier": False,
                    "musee_patrimoine": False,
                    "musique_cd_vynils": False,
                    "musique_live": False,
                    "musique_numerique": False,
                    "pratique_artistique": False,
                    "spectacle_vivant": False,
                    "user_id": "1",
                },
                {
                    "audiovisuel": False,
                    "cinema": True,
                    "instrument": False,
                    "jeux_video": False,
                    "livre_numerique": False,
                    "livre_papier": False,
                    "musee_patrimoine": False,
                    "musique_cd_vynils": False,
                    "musique_live": False,
                    "musique_numerique": False,
                    "pratique_artistique": False,
                    "spectacle_vivant": False,
                    "user_id": "3",
                },
                {
                    "audiovisuel": False,
                    "cinema": True,
                    "instrument": False,
                    "jeux_video": False,
                    "livre_numerique": False,
                    "livre_papier": False,
                    "musee_patrimoine": False,
                    "musique_cd_vynils": False,
                    "musique_live": False,
                    "musique_numerique": False,
                    "pratique_artistique": False,
                    "spectacle_vivant": False,
                    "user_id": "6",
                },
            ],
        )
    ],
)
def test_enriched_booked_categories_data_returns_expected_cinema(
    flush_dataset, table_name, query, input_data, expected
):
    # Given
    create_data(
        client=pytest.bq_client,
        dataset=TEST_DATASET,
        data=input_data,
        table_prefix=TEST_TABLE_PREFIX,
    )

    # When
    run_query(client=pytest.bq_client, query=query)
    output = retrieve_data(
        client=pytest.bq_client, dataset=TEST_DATASET, table=table_name, table_prefix=""
    )

    # Then
    assert output == expected


@pytest.mark.parametrize(
    ["table_name", "query", "input_data", "expected"],
    [
        (
            "enriched_booked_categories_data",
            define_enriched_booked_categories_data_full_query(
                dataset=TEST_DATASET, table_prefix=TEST_TABLE_PREFIX
            ),
            {
                "booking": [
                    {
                        "user_id": "1",
                        "stock_id": "1",
                        "booking_id": "4",
                    }
                ],
                "stock": [{"stock_id": "1", "offer_id": "1"}],
                "offer": [{"offer_id": "1", "offer_type": "ThingType.INSTRUMENT"}],
                "venue": [],
            },
            [
                {
                    "audiovisuel": False,
                    "cinema": False,
                    "instrument": True,
                    "jeux_video": False,
                    "livre_numerique": False,
                    "livre_papier": False,
                    "musee_patrimoine": False,
                    "musique_cd_vynils": False,
                    "musique_live": False,
                    "musique_numerique": False,
                    "pratique_artistique": False,
                    "spectacle_vivant": False,
                    "user_id": "1",
                },
            ],
        )
    ],
)
def test_enriched_booked_categories_data_returns_expected_instrument(
    flush_dataset, table_name, query, input_data, expected
):
    # Given
    create_data(
        client=pytest.bq_client,
        dataset=TEST_DATASET,
        data=input_data,
        table_prefix=TEST_TABLE_PREFIX,
    )

    # When
    run_query(client=pytest.bq_client, query=query)
    output = retrieve_data(
        client=pytest.bq_client, dataset=TEST_DATASET, table=table_name, table_prefix=""
    )

    # Then
    assert output == expected


@pytest.mark.parametrize(
    ["table_name", "query", "input_data", "expected"],
    [
        (
            "enriched_booked_categories_data",
            define_enriched_booked_categories_data_full_query(
                dataset=TEST_DATASET, table_prefix=TEST_TABLE_PREFIX
            ),
            {
                "booking": [
                    {"user_id": "1", "stock_id": "1", "booking_id": "4"},
                    {"user_id": "10", "stock_id": "2", "booking_id": "5"},
                ],
                "stock": [
                    {"stock_id": "1", "offer_id": "1"},
                    {"stock_id": "2", "offer_id": "2"},
                ],
                "offer": [
                    {"offer_id": "1", "offer_type": "ThingType.JEUX_VIDEO"},
                    {"offer_id": "2", "offer_type": "ThingType.JEUX_VIDEO_ABO"},
                ],
                "venue": [],
            },
            [
                {
                    "audiovisuel": False,
                    "cinema": False,
                    "instrument": False,
                    "jeux_video": True,
                    "livre_numerique": False,
                    "livre_papier": False,
                    "musee_patrimoine": False,
                    "musique_cd_vynils": False,
                    "musique_live": False,
                    "musique_numerique": False,
                    "pratique_artistique": False,
                    "spectacle_vivant": False,
                    "user_id": "1",
                },
                {
                    "audiovisuel": False,
                    "cinema": False,
                    "instrument": False,
                    "jeux_video": True,
                    "livre_numerique": False,
                    "livre_papier": False,
                    "musee_patrimoine": False,
                    "musique_cd_vynils": False,
                    "musique_live": False,
                    "musique_numerique": False,
                    "pratique_artistique": False,
                    "spectacle_vivant": False,
                    "user_id": "10",
                },
            ],
        )
    ],
)
def test_enriched_booked_categories_data_returns_expected_jeux_video(
    flush_dataset, table_name, query, input_data, expected
):
    # Given
    create_data(
        client=pytest.bq_client,
        dataset=TEST_DATASET,
        data=input_data,
        table_prefix=TEST_TABLE_PREFIX,
    )

    # When
    run_query(client=pytest.bq_client, query=query)
    output = retrieve_data(
        client=pytest.bq_client, dataset=TEST_DATASET, table=table_name, table_prefix=""
    )

    # Then
    assert output == expected


@pytest.mark.parametrize(
    ["table_name", "query", "input_data", "expected"],
    [
        (
            "enriched_booked_categories_data",
            define_enriched_booked_categories_data_full_query(
                dataset=TEST_DATASET, table_prefix=TEST_TABLE_PREFIX
            ),
            {
                "booking": [
                    {
                        "user_id": "1",
                        "stock_id": "1",
                        "booking_id": "4",
                    }
                ],
                "stock": [{"stock_id": "1", "offer_id": "1"}],
                "offer": [
                    {
                        "offer_id": "1",
                        "offer_type": "ThingType.LIVRE_EDITION",
                        "venue_id": "1",
                    }
                ],
                "venue": [{"venue_id": "1", "venue_is_virtual": True}],
            },
            [
                {
                    "audiovisuel": False,
                    "cinema": False,
                    "instrument": False,
                    "jeux_video": False,
                    "livre_numerique": True,
                    "livre_papier": False,
                    "musee_patrimoine": False,
                    "musique_cd_vynils": False,
                    "musique_live": False,
                    "musique_numerique": False,
                    "pratique_artistique": False,
                    "spectacle_vivant": False,
                    "user_id": "1",
                },
            ],
        )
    ],
)
def test_enriched_booked_categories_data_returns_expected_livre_num(
    flush_dataset, table_name, query, input_data, expected
):
    # Given
    create_data(
        client=pytest.bq_client,
        dataset=TEST_DATASET,
        data=input_data,
        table_prefix=TEST_TABLE_PREFIX,
    )

    # When
    run_query(client=pytest.bq_client, query=query)
    output = retrieve_data(
        client=pytest.bq_client, dataset=TEST_DATASET, table=table_name, table_prefix=""
    )

    # Then
    assert output == expected


@pytest.mark.parametrize(
    ["table_name", "query", "input_data", "expected"],
    [
        (
            "enriched_booked_categories_data",
            define_enriched_booked_categories_data_full_query(
                dataset=TEST_DATASET, table_prefix=TEST_TABLE_PREFIX
            ),
            {
                "booking": [
                    {
                        "user_id": "1",
                        "stock_id": "1",
                        "booking_id": "4",
                    }
                ],
                "stock": [{"stock_id": "1", "offer_id": "1"}],
                "offer": [
                    {
                        "offer_id": "1",
                        "offer_type": "ThingType.LIVRE_EDITION",
                        "venue_id": "1",
                    }
                ],
                "venue": [{"venue_id": "1", "venue_is_virtual": False}],
            },
            [
                {
                    "audiovisuel": False,
                    "cinema": False,
                    "instrument": False,
                    "jeux_video": False,
                    "livre_numerique": False,
                    "livre_papier": True,
                    "musee_patrimoine": False,
                    "musique_cd_vynils": False,
                    "musique_live": False,
                    "musique_numerique": False,
                    "pratique_artistique": False,
                    "spectacle_vivant": False,
                    "user_id": "1",
                },
            ],
        )
    ],
)
def test_enriched_booked_categories_data_returns_expected_livre_papier(
    flush_dataset, table_name, query, input_data, expected
):
    # Given
    create_data(
        client=pytest.bq_client,
        dataset=TEST_DATASET,
        data=input_data,
        table_prefix=TEST_TABLE_PREFIX,
    )

    # When
    run_query(client=pytest.bq_client, query=query)
    output = retrieve_data(
        client=pytest.bq_client, dataset=TEST_DATASET, table=table_name, table_prefix=""
    )

    # Then
    assert output == expected


@pytest.mark.parametrize(
    ["table_name", "query", "input_data", "expected"],
    [
        (
            "enriched_booked_categories_data",
            define_enriched_booked_categories_data_full_query(
                dataset=TEST_DATASET, table_prefix=TEST_TABLE_PREFIX
            ),
            {
                "booking": [
                    {
                        "user_id": "1",
                        "stock_id": "1",
                        "booking_id": "4",
                    },
                    {
                        "user_id": "2",
                        "stock_id": "2",
                        "booking_id": "5",
                    },
                ],
                "stock": [
                    {"stock_id": "1", "offer_id": "1"},
                    {"stock_id": "2", "offer_id": "2"},
                ],
                "offer": [
                    {"offer_id": "1", "offer_type": "EventType.MUSEES_PATRIMOINE"},
                    {"offer_id": "2", "offer_type": "ThingType.MUSEES_PATRIMOINE_ABO"},
                ],
                "venue": [],
            },
            [
                {
                    "audiovisuel": False,
                    "cinema": False,
                    "instrument": False,
                    "jeux_video": False,
                    "livre_numerique": False,
                    "livre_papier": False,
                    "musee_patrimoine": True,
                    "musique_cd_vynils": False,
                    "musique_live": False,
                    "musique_numerique": False,
                    "pratique_artistique": False,
                    "spectacle_vivant": False,
                    "user_id": "1",
                },
                {
                    "audiovisuel": False,
                    "cinema": False,
                    "instrument": False,
                    "jeux_video": False,
                    "livre_numerique": False,
                    "livre_papier": False,
                    "musee_patrimoine": True,
                    "musique_cd_vynils": False,
                    "musique_live": False,
                    "musique_numerique": False,
                    "pratique_artistique": False,
                    "spectacle_vivant": False,
                    "user_id": "2",
                },
            ],
        )
    ],
)
def test_enriched_booked_categories_data_returns_expected_musee_patrimoine(
    flush_dataset, table_name, query, input_data, expected
):
    # Given
    create_data(
        client=pytest.bq_client,
        dataset=TEST_DATASET,
        data=input_data,
        table_prefix=TEST_TABLE_PREFIX,
    )

    # When
    run_query(client=pytest.bq_client, query=query)
    output = retrieve_data(
        client=pytest.bq_client, dataset=TEST_DATASET, table=table_name, table_prefix=""
    )

    # Then
    assert output == expected


@pytest.mark.parametrize(
    ["table_name", "query", "input_data", "expected"],
    [
        (
            "enriched_booked_categories_data",
            define_enriched_booked_categories_data_full_query(
                dataset=TEST_DATASET, table_prefix=TEST_TABLE_PREFIX
            ),
            {
                "booking": [
                    {
                        "user_id": "1",
                        "stock_id": "1",
                        "booking_id": "4",
                    },
                    {
                        "user_id": "2",
                        "stock_id": "2",
                        "booking_id": "4",
                    },
                ],
                "stock": [
                    {"stock_id": "1", "offer_id": "1"},
                    {"stock_id": "2", "offer_id": "2"},
                ],
                "offer": [
                    {"offer_id": "1", "offer_type": "EventType.MUSIQUE"},
                    {"offer_id": "2", "offer_type": "ThingType.MUSIQUE_ABO"},
                ],
                "venue": [],
            },
            [
                {
                    "audiovisuel": False,
                    "cinema": False,
                    "instrument": False,
                    "jeux_video": False,
                    "livre_numerique": False,
                    "livre_papier": False,
                    "musee_patrimoine": False,
                    "musique_cd_vynils": False,
                    "musique_live": True,
                    "musique_numerique": False,
                    "pratique_artistique": False,
                    "spectacle_vivant": False,
                    "user_id": "1",
                },
                {
                    "audiovisuel": False,
                    "cinema": False,
                    "instrument": False,
                    "jeux_video": False,
                    "livre_numerique": False,
                    "livre_papier": False,
                    "musee_patrimoine": False,
                    "musique_cd_vynils": False,
                    "musique_live": True,
                    "musique_numerique": False,
                    "pratique_artistique": False,
                    "spectacle_vivant": False,
                    "user_id": "2",
                },
            ],
        )
    ],
)
def test_enriched_booked_categories_data_returns_expected_musique_live(
    flush_dataset, table_name, query, input_data, expected
):
    # Given
    create_data(
        client=pytest.bq_client,
        dataset=TEST_DATASET,
        data=input_data,
        table_prefix=TEST_TABLE_PREFIX,
    )

    # When
    run_query(client=pytest.bq_client, query=query)
    output = retrieve_data(
        client=pytest.bq_client, dataset=TEST_DATASET, table=table_name, table_prefix=""
    )

    # Then
    assert output == expected


@pytest.mark.parametrize(
    ["table_name", "query", "input_data", "expected"],
    [
        (
            "enriched_booked_categories_data",
            define_enriched_booked_categories_data_full_query(
                dataset=TEST_DATASET, table_prefix=TEST_TABLE_PREFIX
            ),
            {
                "booking": [
                    {
                        "user_id": "1",
                        "stock_id": "1",
                        "booking_id": "4",
                    }
                ],
                "stock": [{"stock_id": "1", "offer_id": "1"}],
                "offer": [
                    {
                        "offer_id": "1",
                        "offer_type": "ThingType.MUSIQUE",
                        "venue_id": "1",
                    }
                ],
                "venue": [{"venue_id": "1", "venue_is_virtual": False}],
            },
            [
                {
                    "audiovisuel": False,
                    "cinema": False,
                    "instrument": False,
                    "jeux_video": False,
                    "livre_numerique": False,
                    "livre_papier": False,
                    "musee_patrimoine": False,
                    "musique_cd_vynils": True,
                    "musique_live": False,
                    "musique_numerique": False,
                    "pratique_artistique": False,
                    "spectacle_vivant": False,
                    "user_id": "1",
                },
            ],
        )
    ],
)
def test_enriched_booked_categories_data_returns_expected_musique_cd_vynils(
    flush_dataset, table_name, query, input_data, expected
):
    # Given
    create_data(
        client=pytest.bq_client,
        dataset=TEST_DATASET,
        data=input_data,
        table_prefix=TEST_TABLE_PREFIX,
    )

    # When
    run_query(client=pytest.bq_client, query=query)
    output = retrieve_data(
        client=pytest.bq_client, dataset=TEST_DATASET, table=table_name, table_prefix=""
    )

    # Then
    assert output == expected


@pytest.mark.parametrize(
    ["table_name", "query", "input_data", "expected"],
    [
        (
            "enriched_booked_categories_data",
            define_enriched_booked_categories_data_full_query(
                dataset=TEST_DATASET, table_prefix=TEST_TABLE_PREFIX
            ),
            {
                "booking": [
                    {
                        "user_id": "1",
                        "stock_id": "1",
                        "booking_id": "4",
                    }
                ],
                "stock": [{"stock_id": "1", "offer_id": "1"}],
                "offer": [
                    {
                        "offer_id": "1",
                        "offer_type": "ThingType.MUSIQUE",
                        "venue_id": "1",
                    }
                ],
                "venue": [{"venue_id": "1", "venue_is_virtual": True}],
            },
            [
                {
                    "audiovisuel": False,
                    "cinema": False,
                    "instrument": False,
                    "jeux_video": False,
                    "livre_numerique": False,
                    "livre_papier": False,
                    "musee_patrimoine": False,
                    "musique_cd_vynils": False,
                    "musique_live": False,
                    "musique_numerique": True,
                    "pratique_artistique": False,
                    "spectacle_vivant": False,
                    "user_id": "1",
                },
            ],
        )
    ],
)
def test_enriched_booked_categories_data_returns_expected_musique_numerique(
    flush_dataset, table_name, query, input_data, expected
):
    # Given
    create_data(
        client=pytest.bq_client,
        dataset=TEST_DATASET,
        data=input_data,
        table_prefix=TEST_TABLE_PREFIX,
    )

    # When
    run_query(client=pytest.bq_client, query=query)
    output = retrieve_data(
        client=pytest.bq_client, dataset=TEST_DATASET, table=table_name, table_prefix=""
    )

    # Then
    assert output == expected


@pytest.mark.parametrize(
    ["table_name", "query", "input_data", "expected"],
    [
        (
            "enriched_booked_categories_data",
            define_enriched_booked_categories_data_full_query(
                dataset=TEST_DATASET, table_prefix=TEST_TABLE_PREFIX
            ),
            {
                "booking": [
                    {
                        "user_id": "1",
                        "stock_id": "1",
                        "booking_id": "4",
                    },
                    {
                        "user_id": "2",
                        "stock_id": "2",
                        "booking_id": "5",
                    },
                ],
                "stock": [
                    {"stock_id": "1", "offer_id": "1"},
                    {"stock_id": "2", "offer_id": "2"},
                ],
                "offer": [
                    {"offer_id": "1", "offer_type": "EventType.PRATIQUE_ARTISTIQUE"},
                    {"offer_id": "2", "offer_type": "ThingType.PRATIQUE_ARTISTIQUE"},
                ],
                "venue": [],
            },
            [
                {
                    "audiovisuel": False,
                    "cinema": False,
                    "instrument": False,
                    "jeux_video": False,
                    "livre_numerique": False,
                    "livre_papier": False,
                    "musee_patrimoine": False,
                    "musique_cd_vynils": False,
                    "musique_live": False,
                    "musique_numerique": False,
                    "pratique_artistique": True,
                    "spectacle_vivant": False,
                    "user_id": "1",
                },
                {
                    "audiovisuel": False,
                    "cinema": False,
                    "instrument": False,
                    "jeux_video": False,
                    "livre_numerique": False,
                    "livre_papier": False,
                    "musee_patrimoine": False,
                    "musique_cd_vynils": False,
                    "musique_live": False,
                    "musique_numerique": False,
                    "pratique_artistique": True,
                    "spectacle_vivant": False,
                    "user_id": "2",
                },
            ],
        )
    ],
)
def test_enriched_booked_categories_data_returns_expected_pratique_artistique(
    flush_dataset, table_name, query, input_data, expected
):
    # Given
    create_data(
        client=pytest.bq_client,
        dataset=TEST_DATASET,
        data=input_data,
        table_prefix=TEST_TABLE_PREFIX,
    )

    # When
    run_query(client=pytest.bq_client, query=query)
    output = retrieve_data(
        client=pytest.bq_client, dataset=TEST_DATASET, table=table_name, table_prefix=""
    )

    # Then
    assert output == expected


@pytest.mark.parametrize(
    ["table_name", "query", "input_data", "expected"],
    [
        (
            "enriched_booked_categories_data",
            define_enriched_booked_categories_data_full_query(
                dataset=TEST_DATASET, table_prefix=TEST_TABLE_PREFIX
            ),
            {
                "booking": [
                    {
                        "user_id": "1",
                        "stock_id": "1",
                        "booking_id": "4",
                    },
                    {
                        "user_id": "2",
                        "stock_id": "2",
                        "booking_id": "5",
                    },
                ],
                "stock": [
                    {"stock_id": "1", "offer_id": "1"},
                    {"stock_id": "2", "offer_id": "2"},
                ],
                "offer": [
                    {"offer_id": "1", "offer_type": "EventType.SPECTACLE_VIVANT"},
                    {"offer_id": "2", "offer_type": "ThingType.SPECTACLE_VIVANT_ABO"},
                ],
                "venue": [],
            },
            [
                {
                    "audiovisuel": False,
                    "cinema": False,
                    "instrument": False,
                    "jeux_video": False,
                    "livre_numerique": False,
                    "livre_papier": False,
                    "musee_patrimoine": False,
                    "musique_cd_vynils": False,
                    "musique_live": False,
                    "musique_numerique": False,
                    "pratique_artistique": False,
                    "spectacle_vivant": True,
                    "user_id": "1",
                },
                {
                    "audiovisuel": False,
                    "cinema": False,
                    "instrument": False,
                    "jeux_video": False,
                    "livre_numerique": False,
                    "livre_papier": False,
                    "musee_patrimoine": False,
                    "musique_cd_vynils": False,
                    "musique_live": False,
                    "musique_numerique": False,
                    "pratique_artistique": False,
                    "spectacle_vivant": True,
                    "user_id": "2",
                },
            ],
        )
    ],
)
def test_enriched_booked_categories_data_returns_expected_spectacle_vivant(
    flush_dataset, table_name, query, input_data, expected
):
    # Given
    create_data(
        client=pytest.bq_client,
        dataset=TEST_DATASET,
        data=input_data,
        table_prefix=TEST_TABLE_PREFIX,
    )

    # When
    run_query(client=pytest.bq_client, query=query)
    output = retrieve_data(
        client=pytest.bq_client, dataset=TEST_DATASET, table=table_name, table_prefix=""
    )

    # Then
    assert output == expected
