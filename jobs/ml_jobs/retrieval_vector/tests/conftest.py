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
from tests.utils import STATIC_FAKE_ITEM_DATA, TRANSFORMER, VECTOR_DIM, VECTOR_SIZE

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def generate_fake_text_item_data() -> pd.DataFrame:
    """
    Encode the 'title' of each item in the input list into vectors using the specified transformer.
    Returns a DataFrame with the original items and an additional 'vector' column.
    """
    model = SentenceTransformer(TRANSFORMER)
    df = pd.DataFrame(STATIC_FAKE_ITEM_DATA)
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
            vector = np.array([(i + 1) / (j + 1) for j in range(vector_dim)])
            price = round(i * 10 + 5, 2)
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

        fake_data = generate_fake_data(
            n=VECTOR_SIZE, vector_dim=VECTOR_DIM, prefix_key="item"
        )
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
            n=VECTOR_SIZE, vector_dim=VECTOR_DIM, prefix_key=prefix_key
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


@pytest.fixture(scope="session")
def mock_user_document_loading(
    generate_fake_data: Callable[[int, int, str], pd.DataFrame],
):
    """
    Fixture to mock load_user_document for both RecoClient and DefaultClient.
    Generates dummy user data and sets it as the return value for the patched methods.
    This is a session-scoped fixture.
    """
    prefix_key = "user"
    user_docs = DocumentArray()
    for _, row in generate_fake_data(
        n=VECTOR_SIZE, vector_dim=VECTOR_DIM, prefix_key=prefix_key
    ).iterrows():
        user_docs.append(Document(id=row[f"{prefix_key}_id"], embedding=row["vector"]))

    logger.info(
        f"Generated {len(user_docs)} user documents for mocking load_user_document."
    )
    if len(user_docs) > 1 and f"{prefix_key}_1" in user_docs:
        logger.info(f"Example user document (user_1): {user_docs[f'{prefix_key}_1']}")

    with (
        patch(
            "app.retrieval.reco_client.RecoClient.load_user_document"
        ) as mock_reco_load,
        patch(
            "app.retrieval.client.DefaultClient.load_user_document"
        ) as mock_default_load,
    ):
        mock_reco_load.return_value = user_docs
        mock_default_load.return_value = user_docs

        logger.info(
            "Patched RecoClient.load_user_document and DefaultClient.load_user_document to return mock user docs."
        )
        yield mock_reco_load, mock_default_load


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
