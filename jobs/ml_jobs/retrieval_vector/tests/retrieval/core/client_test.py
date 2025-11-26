import numpy as np
import pytest
from docarray import Document

from app.retrieval.constants import DISTANCE_COLUMN_NAME
from tests.utils import (
    VECTOR_DIM,
    VECTOR_SIZE,
    calculate_dot_product,
    calculate_l2_distance,
)


@pytest.fixture
def query_vector():
    """Fixture to create a query vector."""
    return Document(embedding=np.array([1] * VECTOR_DIM))


@pytest.fixture
def fake_data(generate_fake_data):
    return generate_fake_data(n=VECTOR_SIZE, vector_dim=VECTOR_DIM, prefix_key="item")


def test_search_by_vector_dot_product(
    mock_connect_db,
    mock_generate_fake_load_item_document,
    mock_user_document_loading,
    fake_data,
    reco_client,
    query_vector,
):
    """Test search by vector using dot product similarity."""
    limit = 10

    fake_data[DISTANCE_COLUMN_NAME] = calculate_dot_product(
        query_vector.embedding, fake_data
    )
    fake_data.sort_values(DISTANCE_COLUMN_NAME, ascending=True, inplace=True)
    result = reco_client.search_by_vector(vector=query_vector, n=limit, details=True)

    assert len(result) == limit

    result_items = [pred["item_id"] for pred in result]
    expected_items = list(fake_data["item_id"].values[:limit])
    assert (
        result_items == expected_items
    ), "Dot product results don't match expected values"


def test_search_by_vector_l2_product(
    mock_connect_db,
    mock_generate_fake_load_item_document,
    mock_user_document_loading,
    fake_data,
    reco_client,
    query_vector,
):
    """Test search by vector using L2 (Euclidean distance) similarity."""
    limit = 10

    fake_data[DISTANCE_COLUMN_NAME] = calculate_l2_distance(
        query_vector.embedding, fake_data
    )
    fake_data.sort_values(DISTANCE_COLUMN_NAME, ascending=True, inplace=True)

    result = reco_client.search_by_vector(vector=query_vector, n=limit, details=True)

    assert len(result) == limit

    result_items = [pred["item_id"] for pred in result]
    expected_items = list(fake_data["item_id"].values[:limit])
    assert (
        result_items == expected_items
    ), "L2 distance results don't match expected values"


def test_search_by_tops(
    mock_connect_db,
    mock_generate_fake_load_item_document,
    mock_user_document_loading,
    reco_client,
):
    """Test search by tops using booking_number_desc as the vector column."""
    limit = 10
    result = reco_client.search_by_tops(
        query_filter={}, n=limit, vector_column_name="booking_number_desc", details=True
    )

    assert len(result) == limit

    # Check if items are sorted by booking_number_desc (which is [0], [1], ... [9])
    result_booking_numbers = [pred["booking_number_desc"][0] for pred in result]
    expected_booking_numbers = list(range(10))

    assert (
        result_booking_numbers == expected_booking_numbers
    ), "Items are not ordered correctly by booking_number_desc"


def test_search_by_tops_rerank(
    mock_connect_db,
    mock_generate_fake_load_item_document,
    mock_user_document_loading,
    reco_client,
):
    """Test search by tops using booking_number_desc as the vector column."""
    limit = 10
    result = reco_client.search_by_tops(
        query_filter={},
        n=limit,
        vector_column_name="booking_number_desc",
        details=True,
        user_id="user_1",
        re_rank=True,
    )

    assert len(result) == limit

    # Check if items are sorted by booking_number_desc (which is [0], [1], ... [9])
    result_booking_numbers = [pred["booking_number_desc"][0] for pred in result]
    expected_booking_numbers = sorted(list(range(10)), reverse=True)

    assert (
        result_booking_numbers == expected_booking_numbers
    ), "Items are not ordered correctly by booking_number_desc"
