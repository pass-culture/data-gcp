import os

TEST_DATASET = f"test_{os.uname().nodename.replace('-', '_')}"  # Temporary => find a better way to make dataset unique
GCP_REGION = "europe-west1"
GCP_PROJECT = "pass-culture-app-projet-test"
BIGQUERY_SCHEMAS = {
    "raw_table": {
        "id": "STRING",
        "float_col": "FLOAT64",
        "string_col": "STRING",
        "unused_col": "STRING",
    }
}
