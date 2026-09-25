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

Two extraction strategies depending on how large a target's candidate
population is — single-shot for `music`/`book`/`movie`/`music_ids`, two-pass
discovery+hydration (with local checkpointing so an Airflow-level retry
resumes instead of restarting) for `gkg`. Full detail, including the SPARQL
template design and why it's shaped the way it is, lives in
[`queries/README.md`](queries/README.md).

## Modules

| File | Responsibility |
|---|---|
| `constants.py` | Every flat extraction constant: entity types, ID-property lists, template filenames, QLever endpoint/headers, checkpoint dir. |
| `wikidata_config.py` | `QUERY_CONFIGS` — the per-target registry (which entity types/ID properties/template(s), single-shot vs. two-pass) — and `render_query()`, which renders a target's Jinja SPARQL template from it. |
| `qlever.py` | Generic QLever HTTP client: retry policy (`tenacity`), cost-rejection detection, raw CSV fetch. No Wikidata-domain logic. |
| `wikidata_extraction.py` | The two-pass discovery+hydration orchestration (`extract_two_pass`/`extract_single_pass`, the two entry points `cli/extraction.py::extract` dispatches to) and entity-ID normalization. |
| `wikidata_checkpoint.py` | Local checkpoint file I/O for a two-pass target's Pass 1 result and hydrated Pass 2 batches. |
| `wikidata_merge.py` | `merge_data`/`postprocess_data` — combines the raw per-target files and normalizes alias columns for `merge`. |

## Dependencies

No optional extras needed — the whole chain only uses what's already in the
job's base dependencies (pandas, loguru, typer, requests, tenacity, jinja2).
`extraction = []` in `pyproject.toml` documents this explicitly rather than
leaving it implicit.

## Tests

`tests/extract_from_wikidata_test.py`, `tests/utils/qlever_test.py`,
`tests/utils/wikidata_checkpoint_test.py`,
`tests/utils/wikidata_extraction_test.py`, `tests/utils/wikidata_merge_test.py`.
