import os

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "passculture-data-ehp")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "dev")
BIGQUERY_RECOMMENDATION_DATASET = f"ml_reco_{ENV_SHORT_NAME}"
BIGQUERY_ANALYTICS_DATASET = f"analytics_{ENV_SHORT_NAME}"
MODELS_RESULTS_TABLE_NAME = "mlflow_training_results"
BIGQUERY_CLEAN_DATASET = f"clean_{ENV_SHORT_NAME}"
LANCE_DB_BATCH_SIZE = 100_000
OUTPUT_DATA_PATH = "./metadata"
MODEL_BASE_PATH = "./model"

ITEM_COLUMNS = [
    "vector",
    "item_id",
    "booking_number_desc",
    "booking_trend_desc",
    "booking_creation_trend_desc",
    "booking_release_trend_desc",
    "raw_embeddings",
    "topic_id",
    "cluster_id",
    "category",
    "subcategory_id",
    "search_group_name",
    "offer_type_label",
    "offer_type_domain",
    "gtl_id",
    "gtl_l1",
    "gtl_l2",
    "gtl_l3",
    "gtl_l4",
    "is_numerical",
    "is_geolocated",
    "is_underage_recommendable",
    "is_restrained",
    "is_sensitive",
    "offer_is_duo",
    "booking_number",
    "booking_number_last_7_days",
    "booking_number_last_14_days",
    "booking_number_last_28_days",
    "semantic_emb_mean",
    "stock_price",
    "offer_creation_date",
    "stock_beginning_date",
    "total_offers",
    "example_offer_id",
    "example_offer_name",
    "example_venue_id",
    "example_venue_latitude",
    "example_venue_longitude",
]
