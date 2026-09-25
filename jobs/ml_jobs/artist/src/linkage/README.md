# Linkage

Matches products (music, book, movie) to artists, deduplicates namesake
artists, and reconciles both against Wikidata.

## Commands

`cli/linkage.py` (also reachable as `main.py linkage ...`):

| Command | What it does |
|---|---|
| `link-new-products` | Incremental update: links newly-seen products to existing artists (raw + preprocessed offer-name matching), creates new artist clusters for unmatched products, and matches those clusters against Wikidata. |
| `refresh-metadata` | Refreshes metadata (description, image, aliases...) for artists already matched to Wikidata, and finds Wikidata matches for artists that don't have one yet. |
| `embed-offer-names` | Encodes offer names for namesake artists' products (via `SentenceTransformer`) as input to `deduplicate`. |
| `deduplicate` | Merges namesake artists whose product-offer-name embeddings are similar enough to be the same real-world artist. |
| `evaluate` | Computes precision/recall/F1 of the matching pipeline against hand-labeled test sets, logged to MLflow. Not run by any DAG — manual/local use. |

```bash
uv run python main.py linkage link-new-products --artist-filepath ... --output-delta-artist-file-path ...
uv run python main.py linkage deduplicate --applicative-artist-filepath ... --output-delta-artist-filepath ...
```

## Modules

| File | Responsibility |
|---|---|
| `constants.py` | Every flat linkage constant: product/artist/delta-table column keys, `ARTISTS_KEYS`/`PRODUCTS_KEYS`/`MUSIC_PLATFORM_IDS_KEYS`, the `Action`/`Comment`/`ProductToLinkStatus` enum-like classes, and the `artist_linkage_config.json`-derived artist-name filter list. |
| `matching.py` | Wikidata category/namesake matching algorithms (`match_artists_with_wikidata`, `perform_wikidata_category_matching`, ...) and `create_artists_tables`/`match_artist_on_offer_names` — the only module here with dedicated unit tests. |
| `product_linking.py` | Business logic behind `link-new-products`: which products to remove/link, building the artist-alias table, sanity checks. |
| `metadata_refresh.py` | Business logic behind `refresh-metadata`: retrieving Wikidata IDs, matching previously-unmatched artists, building the metadata-refresh delta, sanity checks. |
| `deduplication.py` | Namesake detection (`get_namesakes`) and the dedup-matching logic behind `deduplicate`. |
| `preprocessing_utils.py` | Artist-name cleaning/normalization shared across matching, dedup, and metadata refresh. |
| `evaluation.py` | Precision/recall/F1 metric computation behind `evaluate`. |
| `loading.py` | `load_wikidata` — reads the latest Wikidata extraction dump (shared with `similarity`). |
| `clustering_utils.py` | Fuzzy-clustering helpers. Has its own tests but no current caller anywhere in the codebase — orphaned, kept rather than deleted; worth a deliberate call on whether to remove it. |

`src/common/mlflow.py` (lazy-imports `mlflow` so it doesn't break domains that
don't install it) backs `evaluate`'s MLflow logging.

⚠️ **Known bug, not yet fixed** — see [`bug_alert.md`](../../bug_alert.md):
`matching.py`'s `match_artists_with_wikidata` crashes instead of working when
called with its documented default (`artist_with_wiki_ids_df=None`).

## Dependencies

Optional `linkage` extra: `matplotlib`, `mlflow`, `networkx`, `rapidfuzz`,
`scikit-learn`, `sentence-transformers` (the last one shared with `similarity`,
for the HF encoder both domains use).

## Tests

`tests/link_new_products_to_artists_test.py`,
`tests/refresh_artist_metadatas_test.py`, `tests/linkage/matching_test.py`,
`tests/utils/clustering_utils_test.py`, `tests/utils/preprocessing_utils_test.py`.
No test file yet for `deduplicate`, `embed-offer-names`, or `evaluate`.
