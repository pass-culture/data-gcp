from google.cloud.bigquery import SchemaField

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
]

RAW_SCHEMA: list[SchemaField] = STAGING_SCHEMA + _RAW_EXTRA
