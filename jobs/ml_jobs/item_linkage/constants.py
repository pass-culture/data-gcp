MODEL_TYPE = {
    "n_dim": 32,
    "type": "semantic",
    "transformer": "sentence-transformers/all-MiniLM-L6-v2",
    "reducer_pickle_path": "metadata/reducer.pkl",
}
LANCEDB_BATCH_SIZE = 5000
NUM_PARTITIONS = 1024
NUM_SUB_VECTORS = 32
MODEL_PATH = "metadata/vector"
NUM_RESULTS = 20  # Number of results to retrieve
LOGGING_INTERVAL = 50000  # Interval for logging progress

N_PROBES = 20
REFINE_FACTOR = 10

FEATURES = {
    "offer_name": {"method": "jarowinkler", "threshold": 0.95},
    "offer_description": {"method": "jarowinkler", "threshold": 0.95},
    "performer": {"method": "jarowinkler", "threshold": 0.95},
}
MATCHES_REQUIRED = 3
UNKNOWN_PERFORMER = "unkn"
UNKNOWN_NAME = "no_name"
UNKNOWN_DESCRIPTION = "no_des"
INITIAL_LINK_ID = "NC"
