import lancedb
from google.cloud import bigquery
from loguru import logger

from constants import BQ_DATASET, BQ_TABLE, GCP_PROJECT, LANCEDB_TABLE, LANCEDB_URI

# ==========================================
# 1. Configuration
# ==========================================

BQ_TABLE_ID = f"{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

# ==========================================
# 2. Extract 'item_id' from LanceDB
# ==========================================
logger.info("Connecting to LanceDB...")
db = lancedb.connect(LANCEDB_URI)
logger.info("Available tables in LanceDB:")
logger.info(db.table_names())

logger.info(f"Opening table '{LANCEDB_TABLE}' from {LANCEDB_URI}...")
lance_table = db.open_table(LANCEDB_TABLE)

# Extract only the 'item_id' column to save memory and convert to Pandas
logger.info("Extracting item_ids...")
df_items = lance_table.search().select(["id"]).to_pandas()

logger.info(f"Extracted {len(df_items)} rows.")

# ==========================================
# 3. Load Data into BigQuery
# ==========================================
logger.info("Connecting to BigQuery...")
bq_client = bigquery.Client(project=GCP_PROJECT)

# Configure the load job
job_config = bigquery.LoadJobConfig(
    autodetect=True,  # Automatically detect the schema (e.g., STRING or INTEGER)
    write_disposition="WRITE_TRUNCATE",  # Overwrites if table exists. Use WRITE_APPEND to add rows instead.
)

logger.info(f"Creating/loading table {BQ_TABLE_ID} in BigQuery...")
job = bq_client.load_table_from_dataframe(df_items, BQ_TABLE_ID, job_config=job_config)

# Wait for the job to complete
job.result()

logger.info(
    f"Success! Table created. Loaded {job.output_rows} rows into {BQ_TABLE_ID}."
)
