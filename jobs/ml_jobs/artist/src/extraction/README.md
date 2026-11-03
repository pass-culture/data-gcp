# Extraction

Fetches raw artist data from Wikidata via SPARQL (through the public
[QLever](https://qlever.cs.uni-freiburg.de/api/wikidata) endpoint) and merges
the per-target results into one artist table.

## Commands

`cli/extraction.py` (also reachable as `main.py extraction ...`):

| Command | What it does |
|---|---|
| `extract --query-name <name> --output-file-path <path>` | Fetch one target (`music`, `music_ids`, `book`, `movie`, `gkg`) and save its raw rows. Run once per target so a QLever failure on one doesn't force re-fetching the others. |
| `merge --input-dir-path <dir> --output-file-path <path>` | Combine every target's raw file (produced by `extract`) into the final artist table. |

```bash
uv run python main.py extraction extract --query-name music --output-file-path music.parquet
uv run python main.py extraction merge --input-dir-path raw/ --output-file-path artists.parquet
```

## How it works

Each target is fetched with a single QLever query rendered from a Jinja SPARQL
template: full artist attributes for `music`/`book`/`movie`, IDs and matching
score only for `music_ids` and `gkg`. `gkg` only feeds the boolean `gkg`
notability flag, so people found *only* by `gkg` have no name and are dropped
from the final table by design. Full detail, including the SPARQL
template design and why it's shaped the way it is, lives in
[`queries/README.md`](queries/README.md).

## Modules

| File | Responsibility |
|---|---|
| `constants.py` | Every flat extraction constant: entity types, ID-property lists, QLever endpoint/headers. |
| `wikidata_config.py` | `QUERY_CONFIGS` — the per-target registry (which entity types/ID properties/template) — and `render_query()`, which renders a target's Jinja SPARQL template from it. |
| `qlever.py` | Generic QLever HTTP client: retry policy (`tenacity`), raw CSV fetch. No Wikidata-domain logic. |
| `wikidata_extraction.py` | `extract_query`, the entry point `cli/extraction.py::extract` calls, and entity-ID normalization. |
| `wikidata_merge.py` | `merge_data`/`postprocess_data` — combines the raw per-target files and normalizes alias columns for `merge`. |

## Dependencies

No optional extras needed — the whole chain only uses what's already in the
job's base dependencies (pandas, loguru, typer, requests, tenacity, jinja2).
`extraction = []` in `pyproject.toml` documents this explicitly rather than
leaving it implicit.

## Tests

`tests/extract_from_wikidata_test.py`, `tests/utils/qlever_test.py`,
`tests/utils/wikidata_extraction_test.py`, `tests/utils/wikidata_merge_test.py`.
