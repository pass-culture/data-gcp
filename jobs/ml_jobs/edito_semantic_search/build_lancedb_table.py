import lancedb
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
from loguru import logger
# --- 1. Configuration ---
# Replace with your actual GCS URIs
from constants import GCS_DATABASE_URI, GCS_PARQUET_FILE, TABLE_NAME

# Define the batch size for streaming (e.g., 100,000 rows at a time)
BATCH_SIZE = 100000 
VECTOR_DIM = 128 # Replace with your actual vector dimension

# --- 2. The Batch Generator Function ---p

# This function reads the Parquet file from GCS in chunks. 
# The magic is that the pyarrow file system (FS) interface handles the GCS connection, 
# and pq.ParquetFile.iter_batches reads only the requested batch size into memory.
def parquet_batch_generator(parquet_uri: str, batch_size: int):
    """Yields pyarrow.RecordBatch from a Parquet directory stored on GCS or local."""
    import pyarrow.dataset as ds
    try:
        print(f"Streaming data from {parquet_uri} in batches of {batch_size}...")
        dataset = ds.dataset(parquet_uri, format="parquet")
        for batch in dataset.to_batches(batch_size=batch_size):
            yield batch
    except Exception as e:
        print(f"An error occurred during GCS streaming: {e}")
        # Make sure your authentication is set up for pyarrow/gcsfs

# --- 3. Connect to LanceDB and Create Table from Generator ---

db = lancedb.connect(GCS_DATABASE_URI)

# Pass the generator function as the data source!
# LanceDB will consume batches from the generator one at a time and write them
# directly as Lance fragments onto your GCS bucket.
try:
    print(f"Creating LanceDB table '{TABLE_NAME}' on GCS: {GCS_DATABASE_URI}")
    
    # 💡 IMPORTANT: If your table is truly empty, you must use the 'schema' argument 
    # to define the data types, as LanceDB cannot infer the schema from an empty generator. 
    # If the generator yields at least one batch, schema inference usually works.

    table = db.create_table(
        name=TABLE_NAME, 
        data=parquet_batch_generator(GCS_PARQUET_FILE, BATCH_SIZE),
        mode="overwrite"
    )
    
    print(f"\n✅ Table '{TABLE_NAME}' created successfully on GCS via streaming!")
    print(f"Total rows ingested: {table.count_rows()}")
    print("\nTable columns:")
    print(table.schema)
    print("\nSample rows:")
    df = table.search().limit(10).to_pandas()
    print(df.head(10))
    
except Exception as e:
    print(f"A LanceDB error occurred: {e}")