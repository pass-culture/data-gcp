# Semantic Search LanceDB

This job creates a LanceDB vector database from parquet files stored in GCS containing item embeddings.

## Features

- **LanceModel Schema**: Uses pydantic-based LanceModel for easy schema definition and metadata additions
- **Batch Processing**: Streams data in batches to handle large parquet files efficiently (up to 100k+ items per file)
- **Multi-file Support**: Automatically processes all parquet files in a GCS folder
- **Vector Indexing**: Creates IVF_PQ index with cosine distance, optimized for 768-dimension embeddings (embeddinggemma)

## Usage

### Environment Variables

```bash
export LANCEDB_URI="gs://your-bucket/path"
export GCS_EMBEDDING_PARQUET_FILE="gs://your-bucket/embeddings/*.parquet"
export LANCEDB_TABLE="embeddings"
export BATCH_SIZE="100000"
```

### Run the job

```bash
python main.py
```

Or with custom parameters:

```bash
python main.py \
  --gcs-embedding-parquet-file "gs://bucket/embeddings/" \
  --lancedb-uri "gs://bucket/lancedb/" \
  --lancedb-table "my_embeddings" \
  --batch-size 100000
```

## Schema

The LanceDB table uses the following schema:

- `item_id`: string - Unique identifier for each item
- `vector`: Vector(768) - Embedding vector (768 dimensions for embeddinggemma)

The schema is defined as a LanceModel, making it easy to add additional fields or metadata later.
