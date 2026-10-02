# Semantic Search LanceDB

Builds the **semantic item retrieval** LanceDB database from a joined BigQuery
export and publishes it to GCS as an **immutable, versioned artifact**. This is
the single producer of the database that the `retrieval_vector` semantic
endpoint serves — the endpoint resolves the manifest and downloads the current
version from GCS at startup instead of baking it into its image.

Each run writes to its own immutable directory and refreshes a manifest that
readers resolve:

```
<root>/
  versions/
    <version>/        # immutable LanceDB (items.lance/, indexes, …) for this build
  latest.json         # manifest -> { version, uri, table, row_count, created_at }
```

The manifest is published **only after** the versioned DB is fully built and
indexed, so readers atomically switch to a complete artifact. Older versions are
retained (default: 3, `--keep-versions`) for rollback and pruned beyond that.

The output table (`items`) carries the embedding plus the textual / categorical
metadata, and is indexed for vector, full-text and hybrid search.

## Output schema (`items` table)

| Column | Purpose |
|---|---|
| `vector` (float32, fixed-size) | semantic embedding — vector / hybrid search |
| `item_id` | id (BTREE scalar index) — served output + query-item lookup |
| `item_name`, `item_description` | served metadata |
| `search_text` (= name + description) | full-text / hybrid search |
| `category`, `subcategory_id`, `search_group_name` | filterable metadata (BITMAP indexes) |
| `gtl_id`, `gtl_l3`, `gtl_l4` | other item metadata (same as `two_tower` / `graph` retrievals) |
| `is_geolocated`, `booking_number*`, `booking_number_desc`, `total_offers`, `stock_price` (`float64`), `offer_creation_date`, `stock_beginning_date` (`int64` unix-epoch seconds), `semantic_emb_mean` (`float64`) | served item metadata (same as `two_tower` / `graph` retrievals) |
| `example_offer_id`, `example_offer_name`, `example_venue_id`, `example_venue_latitude`, `example_venue_longitude` (`float64`) | served item metadata (same as `two_tower` / `graph` retrievals) |

> ⚠️ `offer_creation_date` / `stock_beginning_date` (BQ `DATE`) and the
> NUMERIC-typed columns (`stock_price`, `example_venue_latitude/longitude`,
> `semantic_emb_mean`) are explicitly cast in the **`EXPORT DATA` BigQuery
> query** (`create_semantic_db` DAG) — `UNIX_SECONDS(TIMESTAMP(...))` (native
> `INT64`) for dates, `CAST(... AS FLOAT64)` for NUMERIC — instead of being
> exported as raw BigQuery DATE/NUMERIC values. Left untouched, they leak as
> exotic strings (e.g. RFC 2822 dates, stringified decimals) once the value
> crosses the gRPC/JSON boundary, breaking the recommendation API's Pydantic
> parsing — this mirrors the two_tower / graph retrieval encoding (`_to_ts` /
> `_to_float` in `retrieval_vector/src/vector_database.py`, which actually
> serves dates as a whole-number epoch, e.g. `"offer_creation_date":
> 1727053696`). Doing the cast in SQL keeps `build_lancedb_table.py` a plain
> passthrough: the parquet already carries the correct types.

## Indexes

- **Vector**: IVF_PQ, cosine. `num_partitions` ≈ `sqrt(n)` capped at 256;
  `num_sub_vectors = dim // 16`.
- **Full-text**: native (Lance) FTS on `search_text` (`use_tantivy=False`, safe on
  object storage). Enables keyword and hybrid search.
- **Scalar**: BTREE on `item_id`, BITMAP on `category` / `subcategory_id` /
  `search_group_name`.

## Usage

The input parquet must already join the embeddings with metadata (see the
`create_semantic_db` DAG, which exports
`ml_semantic_embedding_<env>.all_items_metadata ⋈ ml_input_<env>.item_metadata ⋈ ml_reco_<env>.recommendable_item`,
casting dates to unix-epoch seconds and NUMERIC columns to `FLOAT64`):
`item_id, all_items_metadata_embedding, offer_name, offer_description` plus the
`recommendable_item` metadata columns (`category`, `subcategory_id`,
`search_group_name`, `is_geolocated`, `gtl_id`,
`gtl_l3`, `gtl_l4`, `booking_number*`, `total_offers`, `stock_price`,
`offer_creation_date`, `stock_beginning_date`, `semantic_emb_mean`,
`example_offer_id`, `example_offer_name`, `example_venue_id`,
`example_venue_latitude`, `example_venue_longitude`).


```bash
uv run python main.py \
  --gcs-embedding-parquet-file "gs://bucket/path/to/joined_export/" \
  --lancedb-uri "gs://bucket/semantic_search_lancedb/" \
  --lancedb-table "items" \
  --batch-size 10000 \
  --vector-column-name "all_items_metadata_embedding" \
  --version "20260102T120000" \
  --keep-versions 3
```

`--lancedb-uri` is the artifact **root**: the DB is written to
`<root>/versions/<version>/` and `<root>/latest.json` is refreshed to point at
it. Use a unique, lexicographically-sortable `--version` (the DAG passes the run
`ts_nodash`).

## Warning

- Each run publishes a **new immutable version**; it never overwrites the DB a
  reader may currently be serving. The `latest.json` manifest swap is a single
  small-object write, so readers only ever see a complete artifact and can roll
  back by repointing the manifest to a retained previous version.
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

The DB lives under the current version directory pointed at by the manifest.
Resolve it first, then copy the `items.lance` table:

```bash
ROOT="gs://data-bucket-<ENV_SHORT_NAME>/semantic_search_lancedb"
VERSION_URI=$(gcloud storage cat "$ROOT/latest.json" | python -c 'import json,sys; print(json.load(sys.stdin)["uri"])')
gcloud storage cp --recursive "$VERSION_URI/items.lance" .
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
