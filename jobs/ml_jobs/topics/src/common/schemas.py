"""BigQuery schemas for the vocabulary and labelling domains' output tables
(spec sections 7.1, 7.4). ``topic_item_document``, ``topic``, and
``item_topic`` are per-run output and carry ``MLFLOW_RUN_ID_COLUMN``;
``topic_vocabulary`` is a persistent registry (like item_embedding's config
artifact) and does not.
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

TOPIC_SCHEMA = [
    bigquery.SchemaField("topic_uuid", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("split_level", "INTEGER"),
    bigquery.SchemaField("corpus_id", "STRING"),
    bigquery.SchemaField("corpus_label", "STRING"),
    bigquery.SchemaField("corpus_depth", "INTEGER"),
    bigquery.SchemaField("is_outlier", "BOOLEAN"),
    bigquery.SchemaField("n_items", "INTEGER"),
    bigquery.SchemaField("share_of_corpus", "FLOAT"),
    bigquery.SchemaField("label", "STRING"),
    bigquery.SchemaField(
        "top_words",
        "RECORD",
        mode="REPEATED",
        fields=[
            bigquery.SchemaField("term", "STRING"),
            bigquery.SchemaField("score", "FLOAT"),
            bigquery.SchemaField("count", "INTEGER"),
            bigquery.SchemaField("df", "INTEGER"),
        ],
    ),
    bigquery.SchemaField(
        "gtl_breakdown",
        "RECORD",
        mode="REPEATED",
        fields=[
            bigquery.SchemaField("gtl_id", "STRING"),
            bigquery.SchemaField("n_items", "INTEGER"),
        ],
    ),
    bigquery.SchemaField("run_id", "STRING"),
    bigquery.SchemaField("created_at", "TIMESTAMP"),
    bigquery.SchemaField(MLFLOW_RUN_ID_COLUMN, "STRING"),
]

ITEM_TOPIC_SCHEMA = [
    bigquery.SchemaField("item_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("split_level", "INTEGER"),
    bigquery.SchemaField("corpus_id", "STRING"),
    bigquery.SchemaField("topic_uuid", "STRING"),
    bigquery.SchemaField("is_outlier", "BOOLEAN"),
    bigquery.SchemaField("original_is_outlier", "BOOLEAN"),
    bigquery.SchemaField("assignment", "STRING"),
    bigquery.SchemaField("run_id", "STRING"),
    bigquery.SchemaField(MLFLOW_RUN_ID_COLUMN, "STRING"),
]
