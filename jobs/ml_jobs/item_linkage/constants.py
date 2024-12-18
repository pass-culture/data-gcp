import os

GCP_PROJECT = os.environ.get("PROJECT_NAME")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "")

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

FEATURES = {
    "offer_name": {"method": "jarowinkler", "threshold": 0.90, "missing_value": 0},
    # "offer_description": {"method": "jarowinkler", "threshold": 0.5,"missing_value":0},
    # "performer": {"method": "jarowinkler", "threshold": 0.95,"missing_value":1},
}
MATCHES_REQUIRED = 1
UNKNOWN_PERFORMER = "unkn"
UNKNOWN_NAME = "no_name"
UNKNOWN_DESCRIPTION = "no_des"
INITIAL_LINK_ID = "NC"
