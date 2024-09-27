import logging
import shutil
from typing import Callable
from unittest.mock import patch

import lancedb
import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
from docarray import Document, DocumentArray
from sentence_transformers import SentenceTransformer

from app.retrieval.reco_client import RecoClient

logger = logging.getLogger(__name__)
VECTOR_DIM = 3
TRANSFORMER = "hf-internal-testing/tiny-random-camembert"
DETAIL_COLUMNS = ["vector", "booking_number_desc"]


@pytest.fixture(scope="session")
def generate_fake_text_item_data() -> pd.DataFrame:
    """
    Encode the 'title' of each item in the input list into vectors using the specified transformer.
    Returns a DataFrame with the original items and an additional 'vector' column.
    """
    items = [
        {
            "item_id": 1,
            "booking_number_desc": 1,
            "title": "The Great Gatsby",
            "price": 15.0,
        },
        {"item_id": 2, "booking_number_desc": 2, "title": "Inception", "price": 20.0},
        {
            "item_id": 3,
            "booking_number_desc": 3,
            "title": "The Silent Patient",
            "price": 12.5,
        },
        {
            "item_id": 4,
            "booking_number_desc": 4,
            "title": "The Shawshank Redemption",
            "price": 25.0,
        },
        {"item_id": 5, "booking_number_desc": 5, "title": "Naruto T1", "price": 25.0},
        {
            "item_id": 6,
            "booking_number_desc": 6,
            "title": "The story of a Book",
            "price": 25.0,
        },
    ]
    model = SentenceTransformer(TRANSFORMER)
    df = pd.DataFrame(items)
    df["vector"] = df["title"].apply(lambda title: model.encode(title).tolist())
    return df


@pytest.fixture(scope="session")
def generate_fake_data() -> Callable[[int, int, str], pd.DataFrame]:
    """Generates fake data consisting of vectors and metadata."""

    def _generate(
        n: int = 10, vector_dim: int = VECTOR_DIM, prefix_key: str = "item"
    ) -> pd.DataFrame:
        """
        Inner function to generate the fake data.

        Args:
            n (int): Number of items to generate.
            vector_dim (int): Dimension of the vector.
            prefix_key (str): Prefix for the item ID.

        Returns:
            pd.DataFrame: DataFrame containing generated data.
        """
        data = []
        for i in range(n):
            item_id = f"{prefix_key}_{i}"
            vector = np.random.rand(vector_dim).tolist()
            price = np.round(np.random.rand() * 100, 2)
            data.append(
                {
                    f"{prefix_key}_id": item_id,
                    "vector": vector,
                    "price": price,
                    "booking_number_desc": [i],
                }
            )
        return pd.DataFrame(data)

    return _generate


def create_pyarrow_table(fake_data: pd.DataFrame) -> pa.Table:
    """Converts a pandas DataFrame to a PyArrow Table."""
    schema = pa.schema(
        [
            pa.field("item_id", pa.string()),
            pa.field("vector", pa.list_(pa.float32(), VECTOR_DIM)),
            pa.field("price", pa.float32()),
            pa.field("booking_number_desc", pa.list_(pa.float32(), 1)),
        ]
    )

    item_id_array = pa.array(fake_data["item_id"])
    vector_array = pa.array(
        fake_data["vector"].tolist(), pa.list_(pa.float32(), VECTOR_DIM)
    )
    price_array = pa.array(fake_data["price"].tolist(), pa.float32())
    booking_number_desc_array = pa.array(
        fake_data["booking_number_desc"].tolist(), pa.list_(pa.float32(), 1)
    )

    return pa.Table.from_arrays(
        [item_id_array, vector_array, price_array, booking_number_desc_array],
        schema=schema,
    )


@pytest.fixture(scope="session")
def mock_connect_db(generate_fake_data: Callable[[int, int, str], pd.DataFrame]):
    """Fixture to mock the LanceDB connection and load fake data."""
    with patch("app.retrieval.client.DefaultClient.connect_db") as mock_load:
        uri = "~/.lancedb/"

        db = lancedb.connect(f"{uri}")

        fake_data = generate_fake_data(n=10, vector_dim=3, prefix_key="item")
        arrow_table = create_pyarrow_table(fake_data)

        mock_load.return_value = db.create_table(
            "items", data=arrow_table, exist_ok=True, mode="overwrite"
        )
        yield mock_load

        # Cleanup the temporary database
        try:
            shutil.rmtree(uri)
        except FileNotFoundError:
            pass


@pytest.fixture(scope="session")
def mock_generate_fake_load_item_document(generate_fake_data):
    """Fixture to mock the load_item_document function used in DefaultClient and generate dummy item data."""
    with patch("app.retrieval.client.DefaultClient.load_item_document") as mock_load:
        prefix_key = "item"
        item_docs = DocumentArray()
        for _, row in generate_fake_data(
            n=10, vector_dim=3, prefix_key=prefix_key
        ).iterrows():
            item_docs.append(
                Document(
                    id=row[f"{prefix_key}_id"],
                    embedding=row["vector"],
                    booking_number_desc=row["booking_number_desc"],
                    price=row["price"],
                )
            )
        mock_load.return_value = item_docs

        logger.info(f"Mocked load_item_document with {len(item_docs)} items.")
        yield mock_load


@pytest.fixture(scope="session")
def mock_generate_fake_load_user_document(generate_fake_data):
    """Fixture to mock the load_user_document function used in RecoClient and generate dummy user data."""
    with patch("app.retrieval.reco_client.RecoClient.load_user_document") as mock_load:
        prefix_key = "user"
        user_docs = DocumentArray()
        for _, row in generate_fake_data(
            n=10, vector_dim=3, prefix_key=prefix_key
        ).iterrows():
            user_docs.append(
                Document(id=row[f"{prefix_key}_id"], embedding=row["vector"])
            )
        mock_load.return_value = user_docs

        logger.info(f"Mocked load_user_document with {len(user_docs)} users.")
        logger.info(f"User document: {user_docs['user_1']}")
        yield mock_load


@pytest.fixture
def mock_semantic_load_item_document(generate_fake_text_item_data):
    """Fixture to mock the load_item_document function used in DefaultClient."""

    with patch("app.retrieval.client.DefaultClient.load_item_document") as mock_load:
        item_docs = DocumentArray()
        for _, row in generate_fake_text_item_data.iterrows():
            item_docs.append(
                Document(
                    id=row["item_id"],
                    embedding=row["vector"],
                    booking_number_desc=row["booking_number_desc"],
                    price=row["price"],
                )
            )
        mock_load.return_value = item_docs
        yield mock_load


@pytest.fixture
def mock_semantic_connect_db(generate_fake_text_item_data):
    """Fixture to mock the load_item_document function used in DefaultClient."""

    with patch("app.retrieval.client.DefaultClient.connect_db") as mock_load:
        uri = "~/.lancedb/"
        db = lancedb.connect(f"{uri}")
        mock_load.return_value = db.create_table(
            "items", data=generate_fake_text_item_data, exist_ok=True, mode="overwrite"
        )
        yield mock_load


@pytest.fixture
def reco_client() -> RecoClient:
    """Fixture to initialize the RecoClient."""
    detail_columns = ["vector", "booking_number_desc"]
    client = RecoClient(default_token="[UNK]", detail_columns=detail_columns)
    client.load()
    return client
