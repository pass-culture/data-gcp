"""BigQuery schemas for the vocabulary domain's output tables (spec section
7.1). ``topic_item_document`` is per-run output and carries
``MLFLOW_RUN_ID_COLUMN``; ``topic_vocabulary`` is a persistent registry (like
item_embedding's config artifact) and does not.
"""

from google.cloud import bigquery

from src.common.constants import MLFLOW_RUN_ID_COLUMN

TOPIC_ITEM_DOCUMENT_SCHEMA = [
    bigquery.SchemaField("item_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("gtl_id", "STRING"),
    bigquery.SchemaField("node_id", "STRING"),
    bigquery.SchemaField("lang", "STRING"),
    bigquery.SchemaField("description_hash", "STRING"),
    bigquery.SchemaField("noun_lemmas", "STRING", mode="REPEATED"),
    bigquery.SchemaField("verb_lemmas", "STRING", mode="REPEATED"),
    bigquery.SchemaField("document", "STRING"),
    bigquery.SchemaField(
        "entities",
        "RECORD",
        mode="REPEATED",
        fields=[
            bigquery.SchemaField("text", "STRING"),
            bigquery.SchemaField("label", "STRING"),
        ],
    ),
    bigquery.SchemaField("is_empty", "BOOLEAN"),
    bigquery.SchemaField("is_deleted", "BOOLEAN"),
    bigquery.SchemaField("processing_version", "STRING"),
    bigquery.SchemaField("processed_at", "TIMESTAMP"),
    bigquery.SchemaField(MLFLOW_RUN_ID_COLUMN, "STRING"),
]

TOPIC_VOCABULARY_SCHEMA = [
    bigquery.SchemaField("term_id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("term", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("pos", "STRING"),
    bigquery.SchemaField("df_total", "INTEGER"),
    bigquery.SchemaField(
        "df_by_gtl",
        "RECORD",
        mode="REPEATED",
        fields=[
            bigquery.SchemaField("gtl_id", "STRING"),
            bigquery.SchemaField("df", "INTEGER"),
        ],
    ),
    bigquery.SchemaField("is_stop_word", "BOOLEAN"),
    bigquery.SchemaField("first_seen", "TIMESTAMP"),
    bigquery.SchemaField("last_seen", "TIMESTAMP"),
]
