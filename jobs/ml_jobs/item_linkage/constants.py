import os
from datetime import datetime

GCP_PROJECT = os.environ.get("GCP_PROJECT")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "dev")
EXPERIMENT_NAME = f"item_linkage_v2.0_{ENV_SHORT_NAME}"
DETAIL_COLUMNS = ["item_id"]
SYNCHRO_SUBCATEGORIES = [
    "SUPPORT_PHYSIQUE_MUSIQUE_VINYLE",
    "LIVRE_PAPIER",
    "SUPPORT_PHYSIQUE_MUSIQUE_CD",
    "SEANCE_CINE",
]
METADATA_FEATURES = [
    "item_id",
    "performer",
    "offer_name",
    "offer_description",
    "oeuvre",
    "edition",
    "offer_subcategory_id",
]
EVALUATION_FEATURES = ["item_id", "offer_subcategory_id", "booking_count"]
RUN_NAME = f"run_{datetime.today().strftime('%Y-%m-%d')}"
MLFLOW_RUN_ID_FILENAME = "mlflow_run_id"
RETRIEVAL_FILTERS = ["edition", "offer_subcategory_id"]
BATCH_SIZE_RETRIEVAL = 10000
SEMAPHORE_RETRIEVAL = 100
MODEL_TYPE = {
    "n_dim": 32,
    "type": "semantic",
    "transformer": "sentence-transformers/all-MiniLM-L6-v2",
    "reducer_pickle_path": "metadata/reducer.pkl",
}
PARQUET_BATCH_SIZE = 100000
LANCEDB_BATCH_SIZE = 5000
NUM_PARTITIONS = 1500 if ENV_SHORT_NAME == "prod" else 128
NUM_SUB_VECTORS = 4 if ENV_SHORT_NAME == "prod" else 16
MODEL_PATH = "metadata/vector"
NUM_RESULTS = 5  # Number of results to retrieve
LOGGING_INTERVAL = 50000  # Interval for logging progress

N_PROBES = 5
REFINE_FACTOR = 10

MATCHING_FEATURES = {
    "product": {
        "oeuvre": {"method": "jarowinkler", "threshold": 0.90, "missing_value": 0},
    },
    "offer": {
        "oeuvre": {"method": "jarowinkler", "threshold": 0.95, "missing_value": 0}
    },
}
MATCHES_REQUIRED = 1
UNKNOWN_PERFORMER = "unkn"
UNKNOWN_NAME = "no_name"
UNKNOWN_DESCRIPTION = "no_des"
INITIAL_LINK_ID = "NC"
