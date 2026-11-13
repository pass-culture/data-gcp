# data.gouv.fr exports

passe Culture open data published on [data.gouv.fr](https://www.data.gouv.fr): indicators on beneficiaries, coverage (numerator) and diversity at credit expiration. Technical documentation: [data.gouv.fr exports](https://pass-culture.github.io/data-gcp/dbt/data_gouv/).

| Path | Content |
|---|---|
| `schemas/<resource>/<version>/schema.json` | [Table Schema](https://specs.frictionlessdata.io/table-schema/) of each resource, in French. Generated, do not edit by hand. |
| `build_schemas.py` | Source of the schemas (field titles, definitions, constraints). |
| `export_csv.py` | Exports the BigQuery tables `export_data_gouv_prod.<resource>` to CSV. |
| `publish.py` | Creates or updates the dataset on data.gouv.fr through the API (draft, metadata, resources, schemas), and publishes it. |
| `dataset_description.md` | French description of the dataset, to paste on data.gouv.fr. |
| `organization_description.md` | French presentation of the passe Culture organization on data.gouv.fr (key figures to update each semester). |

The data comes from the dbt models `orchestration/dags/data_gcp_dbt/models/export/data_gouv/exp_data_gouv__<resource>.sql`.

## Publish or update

1. Build and test the tables (from `orchestration/dags/data_gcp_dbt`):

   ```sh
   uv run dbt build --target prod --vars '{ENV_SHORT_NAME: prod}' --select path:models/export/data_gouv
   ```

2. Export the CSV files (UTF-8, comma separated, ISO dates, empty cells for missing values). Data is published **each semester**: `--last-month` is the last published month (June or December); the dbt tables keep every closed month, the cut-off is only applied here.

   ```sh
   uv run python -I data_gouv/export_csv.py --out-dir <dir> --last-month 2026-06
   ```

   | Publication | `--last-month` |
   |---|---|
   | First (test, then real) | `2026-06` |
   | January 2027 | `2026-12` |

3. Check the files against their schemas:

   ```sh
   cd <dir> && uvx frictionless validate --schema <path-to-repo>/data_gouv/schemas/<resource>/1.0.0/schema.json <resource>.csv
   ```

4. Upload with `publish.py`, on [demo.data.gouv.fr](https://demo.data.gouv.fr) first (`--env demo`), then on www (`--env www`). The API key is personal and differs per instance (profile > « Clé d'API »); keep it out of the repo.

   ```sh
   export DATA_GOUV_API_KEY=...
   # first time: creates a draft dataset under the pass-culture organization
   uv run python -I data_gouv/publish.py --env demo --csv-dir <dir>
   # next semesters: updates the dataset, replaces the changed files of the existing resources
   uv run python -I data_gouv/publish.py --env demo --csv-dir <dir> --dataset <id-or-slug>
   ```

   It sets the title and description (`dataset_description.md`), tags, Licence Ouverte 2.0, semiannual frequency, temporal coverage (from the CSV months) and spatial coverage (France, by département), uploads one resource per CSV and declares its Table Schema (raw `schema.json` on GitHub master). Uploads are chunked with retries, as data.gouv.fr drops some requests; it can be re-run, unchanged files are skipped.

   The API creates public datasets by default: the script creates a **draft** (« brouillon »), hidden from the site and search. Its files can still be downloaded by anyone who has their URL.

5. Check the draft: about 15 minutes after the upload, the preview of each resource and, under *Métadonnées > Extras*, that the analysis has no `error` (geographic columns should be detected); the metadata quality score should be complete.

6. Publish:

   ```sh
   uv run python -I data_gouv/publish.py --env www --dataset <id-or-slug> --publish
   ```

## Change a resource

The `exp_data_gouv__*` models have an enforced dbt contract: adding, removing or retyping a column fails the build. A published schema version is immutable:

1. update the model and its `_schema` yml;
2. bump `SCHEMA_VERSION` in `build_schemas.py` (minor for an added column, major for a removed or renamed one), update the field definitions and run `python -I data_gouv/build_schemas.py`;
3. publish the new CSV and update the resource `schema` version and url on data.gouv.fr.
