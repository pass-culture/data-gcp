"""Integration tests for the semantic retrieval flavor.

These build a real (small) LanceDB `items` table — mirroring the schema and the
vector / full-text-search / scalar indexes produced by the ``semantic_search_lancedb``
job — and query it through :class:`SemanticClient`.
"""

from datetime import date

import lancedb
import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

from app.retrieval.constants import DISTANCE_COLUMN_NAME, SCORE_COLUMN_NAME
from app.retrieval.semantic_client import SemanticClient

EMB_SIZE = 16
N_ITEMS = 512  # >= 256 so IVF_PQ / FTS training has enough rows
CATEGORIES = ["LIVRE", "MUSIQUE"]
SUBCATEGORIES = ["LIVRE_PAPIER", "SUPPORT_PHYSIQUE_MUSIQUE"]
SEARCH_GROUP_NAMES = ["LIVRES", "MUSIQUE"]
KEYWORDS = ["roman policier", "concert jazz"]

# Extra `recommendable_item` metadata columns served in `details=True` results,
# for parity with the two_tower / graph retrievals (`DEFAULT_DETAIL_COLUMNS`).
STRING_METADATA_COLUMNS = [
    "search_group_name",
    "topic_id",
    "cluster_id",
    "gtl_id",
    "gtl_l3",
    "gtl_l4",
    "example_offer_id",
    "example_offer_name",
    "example_venue_id",
]
NUMERIC_METADATA_COLUMNS = [
    "booking_number",
    "booking_number_last_7_days",
    "booking_number_last_14_days",
    "booking_number_last_28_days",
    "booking_number_desc",
    "total_offers",
    "stock_price",
    "example_venue_latitude",
    "example_venue_longitude",
]


def _build_items_table(df: pd.DataFrame, emb_size: int, uri: str) -> None:
    """Build a small `items` LanceDB table with the served schema + indexes.

    Mirrors ``semantic_search_lancedb.build_lancedb_table`` (vector IVF_PQ +
    native FTS on ``search_text`` + scalar indexes) at test scale, including
    the ``recommendable_item`` metadata columns (parity with two_tower / graph).
    """
    fields = [
        pa.field("vector", pa.list_(pa.float32(), emb_size)),
        pa.field("item_id", pa.string()),
        pa.field("item_name", pa.string()),
        pa.field("item_description", pa.string()),
        pa.field("search_text", pa.string()),
        pa.field("category", pa.string()),
        pa.field("subcategory_id", pa.string()),
        *(pa.field(col, pa.string()) for col in STRING_METADATA_COLUMNS),
        *(pa.field(col, pa.float64()) for col in NUMERIC_METADATA_COLUMNS),
        pa.field("is_geolocated", pa.bool_()),
        pa.field("offer_creation_date", pa.date32()),
        pa.field("stock_beginning_date", pa.date32()),
        pa.field("semantic_emb_mean", pa.float64()),
    ]
    schema = pa.schema(fields)
    columns = {field.name: df[field.name].tolist() for field in fields}
    columns["vector"] = list(df["vector"])
    data = pa.Table.from_pydict(columns, schema=schema)
    db = lancedb.connect(uri)
    table = db.create_table("items", data=data, mode="overwrite")
    table.create_index(
        metric="cosine",
        num_partitions=2,
        num_sub_vectors=emb_size // 8,
        vector_column_name="vector",
    )
    table.create_fts_index("search_text", use_tantivy=False, replace=True)
    table.create_scalar_index("item_id", index_type="BTREE")
    table.create_scalar_index("category", index_type="BITMAP")
    table.create_scalar_index("subcategory_id", index_type="BITMAP")
    table.create_scalar_index("search_group_name", index_type="BITMAP")


@pytest.fixture(scope="module")
def semantic_db_uri(tmp_path_factory) -> str:
    """Build a small semantic LanceDB table and return its URI."""
    uri = str(tmp_path_factory.mktemp("semantic") / "vector")
    rng = np.random.default_rng(0)
    rows = []
    for i in range(N_ITEMS):
        keyword = KEYWORDS[i % 2]
        row = {
            "item_id": f"item-{i}",
            "vector": rng.random(EMB_SIZE).astype(np.float32),
            "item_name": f"{keyword} numero {i}",
            "item_description": f"une offre autour de {keyword}",
            "category": CATEGORIES[i % 2],
            "subcategory_id": SUBCATEGORIES[i % 2],
            "search_group_name": SEARCH_GROUP_NAMES[i % 2],
            "is_geolocated": bool(i % 2),
            "offer_creation_date": date(2024, 1, 1),
            "stock_beginning_date": date(2024, 1, 1),
            "semantic_emb_mean": float(i),
        }
        for col in STRING_METADATA_COLUMNS:
            row.setdefault(col, f"{col}-{i}")
        for col in NUMERIC_METADATA_COLUMNS:
            row.setdefault(col, float(i))
        rows.append(row)
    df = pd.DataFrame(rows)
    df["search_text"] = df["item_name"] + " " + df["item_description"]
    _build_items_table(df, EMB_SIZE, uri)
    return uri


@pytest.fixture()
def client(semantic_db_uri: str) -> SemanticClient:
    client = SemanticClient(lance_db_uri=semantic_db_uri)
    # Connect the table directly rather than via ``load()``: the session-scoped
    # ``mock_connect_db`` fixture patches ``DefaultClient.connect_db`` for the
    # whole session, which would otherwise hand back the reco fake table.
    client.table = lancedb.connect(semantic_db_uri).open_table("items")
    return client


def test_item_vector_lookup_returns_embedding(client: SemanticClient):
    doc = client.item_vector("item-5")
    assert doc is not None
    assert doc.id == "item-5"
    assert len(doc.embedding) == EMB_SIZE


def test_item_vector_lookup_missing_returns_none(client: SemanticClient):
    assert client.item_vector("does-not-exist") is None


def test_search_by_vector_returns_neighbors(client: SemanticClient):
    query = client.item_vector("item-10")
    results = client.search_by_vector(vector=query, n=5, excluded_items=["item-10"])
    assert 0 < len(results) <= 5
    item_ids = {r["item_id"] for r in results}
    assert "item-10" not in item_ids  # excluded


def test_search_by_text_matches_keyword(client: SemanticClient):
    results = client.search_by_text(text="roman policier", n=10)
    assert len(results) > 0
    # "roman policier" only appears in even-indexed (LIVRE) items
    returned = {r["item_id"] for r in results}
    assert all(int(item_id.split("-")[1]) % 2 == 0 for item_id in returned)


def test_search_by_text_with_category_filter(client: SemanticClient):
    results = client.search_by_text(
        text="concert jazz",
        n=10,
        query_filter={"category": {"$eq": "MUSIQUE"}},
        details=True,
    )
    assert len(results) > 0
    assert all(r["category"] == "MUSIQUE" for r in results)


def test_search_by_text_with_search_group_name_filter(client: SemanticClient):
    """`search_group_name` must be usable as a `params` filter, like the
    two_tower / graph retrievals."""
    results = client.search_by_text(
        text="concert jazz",
        n=10,
        query_filter={"search_group_name": {"$eq": "MUSIQUE"}},
        details=True,
    )
    assert len(results) > 0
    assert all(r["search_group_name"] == "MUSIQUE" for r in results)


def test_search_by_text_details_include_metadata_and_score(client: SemanticClient):
    results = client.search_by_text(text="roman", n=3, details=True)
    assert len(results) > 0
    row = results[0]
    # Same item information as the two_tower / graph retrievals
    # (`DEFAULT_DETAIL_COLUMNS`) plus the semantic-specific name/description.
    for col in (
        "item_id",
        "item_name",
        "item_description",
        "category",
        "subcategory_id",
        "search_group_name",
        "topic_id",
        "cluster_id",
        "booking_number",
        "semantic_emb_mean",
    ):
        assert col in row
    assert SCORE_COLUMN_NAME in row


def test_search_by_vector_details_include_distance(client: SemanticClient):
    query = client.item_vector("item-1")
    results = client.search_by_vector(vector=query, n=3, details=True)
    assert len(results) > 0
    assert DISTANCE_COLUMN_NAME in results[0]


def test_non_detail_results_are_minimal(client: SemanticClient):
    results = client.search_by_text(text="concert", n=3, details=False)
    assert len(results) > 0
    assert set(results[0].keys()) == {"idx", "item_id", "_score"}
