from src.common.constants import MLFLOW_RUN_ID_COLUMN
from src.common.schemas import TOPIC_ITEM_DOCUMENT_SCHEMA, TOPIC_VOCABULARY_SCHEMA


def test_topic_item_document_has_mlflow_run_id_column() -> None:
    names = [field.name for field in TOPIC_ITEM_DOCUMENT_SCHEMA]
    assert MLFLOW_RUN_ID_COLUMN in names
    assert "item_id" in names


def test_topic_vocabulary_has_no_mlflow_run_id_column() -> None:
    """topic_vocabulary is a persistent registry, not per-run output, so it
    isn't stamped with mlflow_run_id (matches item_embedding's choice to only
    stamp provenance on the embeddings table, never on config/registry
    tables).
    """
    names = [field.name for field in TOPIC_VOCABULARY_SCHEMA]
    assert MLFLOW_RUN_ID_COLUMN not in names
    assert "term_id" in names
