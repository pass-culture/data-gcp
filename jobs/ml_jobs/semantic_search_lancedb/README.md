# Semantic Search LanceDB

Creates a LanceDB vector database from Parquet embedding files on GCS. Outputs an IVF_PQ-indexed table with `item_id` and `vector` columns.

## Features

- **Batch streaming**: Streams Parquet data in configurable batches to handle large datasets
- **Multi-file support**: Processes all Parquet files in a GCS folder
- **Auto-scaled indexing**: IVF_PQ index with cosine distance; `num_partitions` scales with dataset size (`sqrt(n)`)
- **Schema normalization**: Handles PyArrow/LanceDB list field naming mismatch automatically

## Usage

```bash
uv run python main.py \
  --gcs-embedding-parquet-file "gs://bucket/path/to/embeddings/" \
  --lancedb-uri "gs://bucket/lancedb/env" \
  --lancedb-table "item_embeddings" \
  --batch-size 10000 \
  --vector-column-name "semantic_content_sts"
```

### Parameters

| Parameter | Description |
|---|---|
| `--gcs-embedding-parquet-file` | GCS path to Parquet file(s) containing embeddings |
| `--lancedb-uri` | GCS URI for the LanceDB database |
| `--lancedb-table` | Name of the table to create |
| `--batch-size` | Number of rows per batch during ingestion |
| `--vector-column-name` | Name of the embedding column in the source Parquet (renamed to `vector`) |

## Warning

- This job **drops and recreates** the LanceDB table if it already exists. If you use this table in a latency-sensitive service, consider writing to a staging table and swapping.
- The indexing takes about 10 to 15 minutes. So do not worry if you don't get frequet logs during indexing.
