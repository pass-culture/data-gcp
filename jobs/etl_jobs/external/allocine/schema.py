import logging

from google.cloud.bigquery import SchemaField

logger = logging.getLogger(__name__)

# Columns serialized to JSON strings for BigQuery compatibility
COMPLEX_COLUMNS = ["releases", "countries", "genres", "companies", "cast_normalized", "credits_normalized"]

# Ordered column list for the staging table (API output + content_hash)
STAGING_COLUMNS = [
    "movie_id",
    "internalId",
    "title",
    "originalTitle",
    "type",
    "runtime",
    "synopsis",
    "poster_url",
    "backlink_url",
    "backlink_label",
    "data_eidr",
    "data_productionYear",
    "cast_normalized",
    "credits_normalized",
    "releases",
    "countries",
    "genres",
    "companies",
    "content_hash",
]

# BigQuery schema for staging_movies — mirrors API output + content_hash
STAGING_SCHEMA: list[SchemaField] = [
    SchemaField("movie_id", "STRING", mode="REQUIRED"),
    SchemaField("internalId", "STRING"),
    SchemaField("title", "STRING"),
    SchemaField("originalTitle", "STRING"),
    SchemaField("type", "STRING"),
    SchemaField("runtime", "INTEGER"),
    SchemaField("synopsis", "STRING"),
    SchemaField("poster_url", "STRING"),
    SchemaField("backlink_url", "STRING"),
    SchemaField("backlink_label", "STRING"),
    SchemaField("data_eidr", "STRING"),
    SchemaField("data_productionYear", "INTEGER"),
    SchemaField("cast_normalized", "STRING"),
    SchemaField("credits_normalized", "STRING"),
    SchemaField("releases", "STRING"),
    SchemaField("countries", "STRING"),
    SchemaField("genres", "STRING"),
    SchemaField("companies", "STRING"),
    SchemaField("content_hash", "STRING"),
]

# Extra columns on raw_movies for poster download tracking
_RAW_EXTRA: list[SchemaField] = [
    SchemaField("poster_to_download", "BOOL"),
    SchemaField("poster_gcs_path", "STRING"),
    SchemaField("retry_count", "INTEGER"),
    SchemaField("updated_at", "TIMESTAMP"),
]

RAW_EXTRA_INIT_VALUES = {
    "poster_to_download": "TRUE",
    "poster_gcs_path": "NULL",
    "retry_count": "0",
    "updated_at": "CURRENT_TIMESTAMP()",
}

# --- Hard check: raise if any extra field has no default ---
missing_defaults = [f.name for f in _RAW_EXTRA if f.name not in RAW_EXTRA_INIT_VALUES]
if missing_defaults:
    raise ValueError(f"No init values defined for raw schema extra columns: {missing_defaults}")

RAW_SCHEMA: list[SchemaField] = STAGING_SCHEMA + _RAW_EXTRA
