import lancedb
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

# --- 1. Configuration ---
# Replace with your actual GCS URIs
from constants import LANCEDB_URI, GCS_EMBEDDING_PARQUET_FILE, LANCEDB_TABLE

# Define the batch size for streaming (e.g., 100,000 rows at a time)
BATCH_SIZE = 100000



# This function reads the Parquet file from GCS in chunks.
# The magic is that the pyarrow file system (FS) interface handles the GCS connection,
# and pq.ParquetFile.iter_batches reads only the requested batch size into memory.
def parquet_batch_generator(parquet_uri: str, batch_size: int):
    """Yields pyarrow.RecordBatch from a Parquet directory stored on GCS or local."""
    import pyarrow.dataset as ds

    try:
        print(f"Streaming data from {parquet_uri} in batches of {batch_size}...")
        dataset = ds.dataset(parquet_uri, format="parquet")
        
        # Print schema for debugging
        print(f"\nOriginal schema:")
        for field in dataset.schema:
            print(f"  {field.name}: {field.type}")
        print()
        
        for batch in dataset.to_batches(batch_size=batch_size):
            # Convert nested structures to LanceDB-compatible format
            yield batch
    except Exception as e:
        print(f"An error occurred during GCS streaming: {e}")
        import traceback
        traceback.print_exc()
        # Make sure your authentication is set up for pyarrow/gcsfs



# --- 4. Connect to LanceDB and Create Table from Generator ---

if __name__ == "__main__":
    print("=" * 60)
    print("LanceDB Table Creation from GCS Parquet")
    print("=" * 60)
    
    # First, inspect the schema
    print('\n[1/3] Reading schema from:', GCS_EMBEDDING_PARQUET_FILE)
    try:
        dataset = ds.dataset(GCS_EMBEDDING_PARQUET_FILE, format='parquet')
        print('\nSchema:')
        print(dataset.schema)
        print('\n--- Field details ---')
        for field in dataset.schema:
            print(f'  {field.name}: {field.type}')
    except Exception as e:
        print(f"Error reading schema: {e}")
        import traceback
        traceback.print_exc()
        exit(1)

    # Connect to LanceDB
    print(f"\n[2/4] Connecting to LanceDB at: {LANCEDB_URI}")
    db = lancedb.connect(LANCEDB_URI)

    # Check if table exists and drop it
    print(f"\n[3/4] Checking for existing table '{LANCEDB_TABLE}'...")
    try:
        existing_tables = db.table_names()
        if LANCEDB_TABLE in existing_tables:
            print(f"Table '{LANCEDB_TABLE}' exists. Dropping...")
            db.drop_table(LANCEDB_TABLE)
            print(f"      ✓ Table dropped successfully")
        else:
            print(f"      ✓ No existing table found")
    except Exception as e:
        print(f"      Warning: Could not check/drop table: {e}")

    # Pass the generator function as the data source!
    # LanceDB will consume batches from the generator one at a time and write them
    # directly as Lance fragments onto your GCS bucket.
    try:
        print(f"\n[4/4] Creating LanceDB table '{LANCEDB_TABLE}' on GCS...")
        print(f"      Batch size: {BATCH_SIZE}")

        table = db.create_table(
            name=LANCEDB_TABLE,
            data=parquet_batch_generator(GCS_EMBEDDING_PARQUET_FILE, BATCH_SIZE),
        )

        print(f"\n{'=' * 60}")
        print(f"SUCCESS: Table '{LANCEDB_TABLE}' created on GCS!")
        print(f"{'=' * 60}")
        print(f"Total rows ingested: {table.count_rows():,}")
        print("\nTable schema:")
        print(table.schema)
        print("\nSample rows:")
        df = table.search().limit(10).to_pandas()
        print(df.head(10))

    except Exception as e:
        print(f"\n{'=' * 60}")
        print(f"ERROR: LanceDB table creation failed")
        print(f"{'=' * 60}")
        print(f"Error message: {e}")
        import traceback
        traceback.print_exc()
        exit(1)
