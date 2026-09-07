# Semantic Search LanceDB

Builds the **semantic item retrieval** LanceDB database from a joined BigQuery
export and publishes it to GCS. This is the single producer of the database that
the `retrieval_vector` semantic endpoint serves — the endpoint downloads it from
GCS at startup instead of baking it into its image.

The output table (`items`) carries the embedding plus the textual / categorical
metadata, and is indexed for vector, full-text and hybrid search.

## Output schema (`items` table)

| Column | Purpose |
|---|---|
| `vector` (float32, fixed-size) | semantic embedding — vector / hybrid search |
| `item_id` | id (BTREE scalar index) — served output + query-item lookup |
| `item_name`, `item_description` | served metadata |
| `search_text` (= name + description) | full-text / hybrid search |
| `category`, `subcategory_id` | filterable metadata (BITMAP indexes) |

## Indexes

- **Vector**: IVF_PQ, cosine. `num_partitions` ≈ `sqrt(n)` capped at 256;
  `num_sub_vectors = dim // 16`.
- **Full-text**: native (Lance) FTS on `search_text` (`use_tantivy=False`, safe on
  object storage). Enables keyword and hybrid search.
- **Scalar**: BTREE on `item_id`, BITMAP on `category` / `subcategory_id`.

## Usage

The input parquet must already join the embeddings with metadata (see the
`semantic_search_lancedb` DAG, which exports
`ml_feat_<env>.item_embedding_refactor ⋈ ml_input_<env>.item_metadata`):
`item_id, semantic_content, offer_name, offer_description, offer_category_id,
offer_subcategory_id`.

```bash
uv run python main.py \
  --gcs-embedding-parquet-file "gs://bucket/path/to/joined_export/" \
  --lancedb-uri "gs://bucket/semantic_search_lancedb/" \
  --lancedb-table "items" \
  --batch-size 10000 \
  --vector-column-name "semantic_content"
```

## Warning

- This job **drops and recreates** the LanceDB table if it already exists. Since a
  latency-sensitive endpoint downloads this DB at startup, redeploys pick up the
  new data; in-place readers would see the swap.
- Indexing might take ~10–15 minutes with few logs — this is expected.
