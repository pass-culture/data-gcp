import lancedb
import pyarrow.dataset as ds
import typer

# --- 1. Configuration ---
# Replace with your actual GCS URIs

# Define the batch size for streaming (e.g., 100,000 rows at a time)
BATCH_SIZE = 100000


# This function reads the Parquet file from GCS in chunks.
# The magic is that the pyarrow file system (FS) interface handles the GCS connection,
# and pq.ParquetFile.iter_batches reads only the requested batch size into memory.
def parquet_batch_generator(parquet_uri: str, batch_size: int):
    """Yields pyarrow.RecordBatch from a Parquet directory stored on GCS or local."""

    try:
        print(f"Streaming data from {parquet_uri} in batches of {batch_size}...")
        dataset = ds.dataset(parquet_uri, format="parquet")

        # Print schema for debugging
        print("\nOriginal schema:")
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


def build_lancedb_table(
    gcs_embedding_parquet_file: str = typer.Option(..., help="GCS Parquet file path"),
    lancedb_uri: str = typer.Option(..., help="LanceDB URI"),
    lancedb_table: str = typer.Option(..., help="LanceDB table name"),
    batch_size: int = typer.Option(..., help="Batch size for streaming"),
):
    print("=" * 60)
    print("LanceDB Table Creation from GCS Parquet")
    print("=" * 60)

    # Connect to LanceDB
    print(f"\n[2/4] Connecting to LanceDB at: {lancedb_uri}")
    db = lancedb.connect(lancedb_uri)

    # Check if table exists and drop it
    print(f"\n[3/4] Checking for existing table '{lancedb_table}'...")
    try:
        existing_tables = db.table_names()
        if lancedb_table in existing_tables:
            print(f"Table '{lancedb_table}' exists. Dropping...")
            db.drop_table(lancedb_table)
            print("      ✓ Table dropped successfully")
        else:
            print("      ✓ No existing table found")
    except Exception as e:
        print(f"      Warning: Could not check/drop table: {e}")

    # Pass the generator function as the data source!
    # LanceDB will consume batches from the generator one at a time and write them
    # directly as Lance fragments onto your GCS bucket.
    try:
        print(f"\n[4/4] Creating LanceDB table '{lancedb_table}' on GCS...")
        print(f"      Batch size: {batch_size}")

        table = db.create_table(
            name=lancedb_table,
            data=parquet_batch_generator(gcs_embedding_parquet_file, batch_size),
        )

        print(f"\n{'=' * 60}")
        print(f"SUCCESS: Table '{lancedb_table}' created on GCS!")
        print(f"{'=' * 60}")
        print(f"Total rows ingested: {table.count_rows():,}")
        print("\nTable schema:")
        print(table.schema)
        print("\nSample rows:")

        ## for testing:
        # df = table.search().limit(10).to_pandas()
        # print(df.head(10))

    except Exception as e:
        print(f"\n{'=' * 60}")
        print("ERROR: LanceDB table creation failed")
        print(f"{'=' * 60}")
        print(f"Error message: {e}")
        import traceback

        traceback.print_exc()
        exit(1)


if __name__ == "__main__":
    typer.run(build_lancedb_table)
