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


## The resulting lancedb Tabl `items.lance`

### Schema (7 columns, 4 945 325 rows @ version 6)

| Column             | Type                             | Notes                          |
| ------------------ | -------------------------------- | ------------------------------ |
| `vector`           | `fixed_size_list<float>[768]`    | Semantic embedding             |
| `item_id`          | `string`                         | Unique item identifier         |
| `item_name`        | `string`                         | Human-readable name            |
| `item_description` | `string`                         | Free text (often empty)        |
| `search_text`      | `string`                         | Concatenated text for FTS      |
| `category`         | `string`                         | Coarse category (14 values)    |
| `subcategory_id`   | `string`                         | Fine category (67 values)      |

### Indexes

| Index name           | Type      | Column           | Best for                                              |
| -------------------- | --------- | ---------------- | ----------------------------------------------------- |
| `vector_idx`         | **IVF_PQ** (cosine) | `vector`   | Approximate nearest-neighbour KNN                     |
| `search_text_idx`    | **FTS**   | `search_text`    | Full-text / BM25 keyword search                       |
| `item_id_idx`        | **BTree** | `item_id`        | Point lookups, `=`, `IN (...)`, range                 |
| `category_idx`       | **Bitmap**| `category`       | Fast equality / `IN` filters on the 14 categories     |
| `subcategory_id_idx` | **Bitmap**| `subcategory_id` | Fast equality / `IN` filters on the 67 subcategories  |

### Possible filters (SQL-like syntax passed to `.where(...)`)

- Comparisons: `=`, `!=`, `<`, `<=`, `>`, `>=`
- Sets: `col IN ('a', 'b')`, `col NOT IN (...)`
- Boolean: `AND`, `OR`, `NOT`
- Strings: `LIKE 'foo%'`, `col IS NULL`, `col IS NOT NULL`
- Grouping: parentheses

### Categories (14) and top subcategories (67)

**Categories (14):**
```LIVRE, MUSIQUE_ENREGISTREE, CINEMA, BEAUX_ARTS, FILM, INSTRUMENT, SPECTACLE, MUSIQUE_LIVE, PRATIQUE_ART, MUSEE, CONFERENCE, JEU, MEDIA, CARTE_JEUNES```

**Subcategories (67 distinct), top 20:**
```LIVRE_PAPIER, SUPPORT_PHYSIQUE_MUSIQUE_CD, SUPPORT_PHYSIQUE_MUSIQUE_VINYLE, MATERIEL_ART_CREATIF, SEANCE_CINE, SUPPORT_PHYSIQUE_FILM, ACHAT_INSTRUMENT, SPECTACLE_REPRESENTATION, CONCERT, FESTIVAL_MUSIQUE, ABO_PRATIQUE_ART, PARTITION, ATELIER_PRATIQUE_ART, LIVRE_NUMERIQUE, RENCONTRE, VISITE_GUIDEE, VISITE, EVENEMENT_PATRIMOINE, CARTE_CINE_MULTISEANCES, FESTIVAL_SPECTACLE```

### Examples querying the db in `python`
**1. Download the lancedb table from GCS in the working directory**
> caution: the db is about 20G, consider launching a VM if you need more space

```gcloud storage cp --recursive \
  "gs://data-bucket-<ENV_SHORT_NAME>/semantic_search_lancedb/items.lance" \
  .
```
**2. Connect to the lancedb**
```
import lancedb
db = lancedb.connect(".")
table = db.open_table("items")
```

**3. Get DB schema**
```
table.schema
table.list_indices()
```

**4. Examples queries**

4.1 Exact lookup on the BTree-indexed item_id
```
SCALAR_COLS = [
            "item_id",
            "item_name",
            "item_description",
            "search_text",
            "category",
            "subcategory_id",
            ]

table.search()
.where("item_id = <item_id>")
.select(SCALAR_COLS)
.limit(5)
.to_pandas()
```

4.2 Bitmap-indexed category + subcategory filter
```
table.search()
.where(
    "category = 'MUSIQUE_ENREGISTREE' "
    "AND subcategory_id = 'SUPPORT_PHYSIQUE_MUSIQUE_VINYLE'"
)
.select(SCALAR_COLS)
.limit(5)
.to_pandas()
```

4.3 IN filter on subcategory_id
```
table.search()
.where("subcategory_id IN ('CONCERT', 'FESTIVAL_MUSIQUE')")
.select(SCALAR_COLS)
.limit(5)
.to_pandas()
```

4.4 KNN with a vector + filtering on scalar index

```

# Get 100th vector of the DB
query_vec = table.search().select(["vector", "item_name"]).limit(100).to_df().iloc[-1]['vector']

table.search(query_vec)
.metric("cosine")
.where("category = 'LIVRE'", prefilter=True)
.limit(5)
.select(["item_id", "item_name", "category", "subcategory_id"])
.to_pandas()

```

4.5 Full-Text Search

```
table.search("concert jazz", query_type="fts")
.where("category = 'MUSIQUE_LIVE'")
.limit(5)
.select(["item_id", "item_name", "subcategory_id"])
.to_pandas()
```
