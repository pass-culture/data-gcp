# Artist

Links products (music, book, movie) to artists, enriches them with Wikidata
metadata, deduplicates namesakes, and builds the artist-similarity index used
for recommendations.

The documentation of this track is available in this **[Notion page](https://www.notion.so/passcultureapp/Artist-Linkage-9a83bb4e274c49e0894e2993e9f831e9)**.

## Architecture

The job is split into three domains, each with its own CLI, business-logic
modules, and dependency group — see each domain's own README for details:

| Domain | README | What it does |
|---|---|---|
| **extraction** | [`src/extraction/README.md`](src/extraction/README.md) | Fetches raw artist data from Wikidata (SPARQL via QLever). |
| **linkage** | [`src/linkage/README.md`](src/linkage/README.md) | Matches products to artists, deduplicates namesakes, reconciles against Wikidata. |
| **similarity** | [`src/similarity/README.md`](src/similarity/README.md) | Enriches artists with biography/image content, builds the similarity index. |

```
artist/
├── main.py              # root entrypoint: main.py <domain> <command> ...
├── cli/                  # one thin file per domain — argument parsing + I/O only
│   ├── extraction.py
│   ├── linkage.py
│   └── similarity.py
├── src/
│   ├── common/            # shared infra: env/GCP/mlflow config, cross-domain constants
│   ├── extraction/         # business logic + SPARQL query templates (queries/)
│   ├── linkage/
│   └── similarity/
└── tests/
```

Every command is reachable two ways — directly (`cli/<domain>.py <command>`) or
through the root entrypoint (`main.py <domain> <command>`); the Airflow DAGs use
the latter.

```bash
uv run python main.py extraction extract --query-name music --output-file-path music.parquet
uv run python main.py linkage deduplicate --applicative-artist-filepath ... --output-delta-artist-filepath ...
uv run python main.py similarity encode-biographies --artist-with-biography-file-path ... --output-file-path ...
```

## Dependencies

Base dependencies (pandas, typer, loguru, GCP clients...) are always
installed; each domain's heavier packages (mlflow, scikit-learn, lancedb,
sentence-transformers...) sit behind its own optional group in
`pyproject.toml` (`extraction`, `linkage`, `similarity`) — install only what
you need: `uv sync --extra linkage`, or `uv sync --all-extras` for everything.

## Configuration

`src/linkage/artist_linkage_config.json` holds linkage-specific preprocessing
rules (currently: artist names to filter out of matching). Loaded into
`src/linkage/constants.py`'s `ARTIST_NAME_TO_FILTER`.

## Testing

```bash
make test
```

Runs the full suite (`PYTHONPATH=. uv run pytest tests`) across all three
domains. See each domain's README for which of its commands currently have
test coverage.

## Known issues

[`bug_alert.md`](bug_alert.md) documents a real, not-yet-fixed bug in
`src/linkage/matching.py`.
