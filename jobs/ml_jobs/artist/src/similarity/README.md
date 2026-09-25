# Similarity

Enriches matched artists with biography/image content and builds the
artist-to-artist similarity index used for recommendations.

## Commands

`cli/similarity.py` (also reachable as `main.py similarity ...`):

| Command | What it does |
|---|---|
| `get-wikipedia-content` | Fetches and cleans Wikipedia article text for artists matched to Wikidata. |
| `summarize-biographies` | Summarizes fetched Wikipedia content into a short artist biography via LLM (pydantic-ai). |
| `get-wikimedia-license` | Fetches Wikimedia Commons license metadata for artist images. |
| `transfer-images` | Downloads artist images from Wikimedia and uploads them (compressed to JPEG) to GCS. |
| `encode-biographies` | Enriches each artist's biography with Wikidata attributes (genres, professions, birth decade...) and encodes the result to an embedding. |
| `create-similar-artist-parquet` | Builds a LanceDB vector index over artist embeddings and computes each artist's top-10 most similar artists (semantic + two-tower item embeddings, merged by Reciprocal Rank Fusion). |

```bash
uv run python main.py similarity encode-biographies --artist-with-biography-file-path ... --output-file-path ...
uv run python main.py similarity create-similar-artist-parquet --artist-with-embeddings-file-path ... --output-file-path ...
```

## Modules

| File | Responsibility |
|---|---|
| `constants.py` | Every flat similarity constant: Wikidata image/license keys, biography/embedding keys, the Wikimedia request header. |
| `wikipedia_content.py` | Business logic behind `get-wikipedia-content`: MediaWiki API fetch + wikitext cleanup, incremental-vs-from-scratch filtering. |
| `llm.py` / `llm_config.py` | LLM biography summarization (behind `summarize-biographies`) and the pydantic-ai model/prompt config. |
| `wikimedia_license.py` | Business logic behind `get-wikimedia-license`: Commons API license lookup, allowed-license filtering. |
| `wikimedia_transfer.py` | Business logic behind `transfer-images`: pooled HTTP session + GCS client, image compression, parallel transfer. |
| `biography_enrichment.py` | Business logic behind the enrichment half of `encode-biographies`: merging Wikidata attributes into one biography text field. |
| `embedding.py` | The embedding half of `encode-biographies`: `SentenceTransformer` encoding (shared HF model with `linkage`'s dedup). |
| `vector_search.py` | Business logic behind `create-similar-artist-parquet`: LanceDB search, Reciprocal Rank Fusion merge, result formatting. |

## Dependencies

Optional `similarity` extra: `lancedb`, `mwparserfromhell`, `networkx`,
`pillow`, `pydantic-ai`, `sentence-transformers` (the last one shared with
`linkage`).

## Tests

`tests/get_wikipedia_page_content_test.py`,
`tests/summarize_biographies_with_llm_test.py`,
`tests/create_similar_artist_parquet_test.py`. No test file yet for
`get-wikimedia-license`, `transfer-images`, or `encode-biographies`.
