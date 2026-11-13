# data.gouv.fr exports

pass Culture open data published on [data.gouv.fr](https://www.data.gouv.fr): indicators on beneficiaries, coverage (numerator) and diversity at credit expiration. Technical documentation: [data.gouv.fr exports](https://pass-culture.github.io/data-gcp/dbt/data_gouv/).

| Path | Content |
|---|---|
| `schemas/<resource>/<version>/schema.json` | [Table Schema](https://specs.frictionlessdata.io/table-schema/) of each resource, in French. Generated, do not edit by hand. |
| `build_schemas.py` | Source of the schemas (field titles, definitions, constraints). |
| `export_csv.py` | Exports the BigQuery tables `export_data_gouv_prod.<resource>` to CSV. |
| `dataset_description.md` | French description of the dataset, to paste on data.gouv.fr. |

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

4. Upload on [demo.data.gouv.fr](https://demo.data.gouv.fr) first, then on www.data.gouv.fr: one resource per CSV, dataset description from `dataset_description.md`, tags `culture`, `ministeredelaculture`, `pass-culture`, `jeunesse`, `pratiques-culturelles`, licence Licence Ouverte 2.0.

5. Declare each resource schema (`name`, `version`, `url` of the raw `schema.json`) through the data.gouv.fr API, e.g.:

   ```sh
   curl -X PUT "https://www.data.gouv.fr/api/1/datasets/<dataset_id>/resources/<resource_id>/" \
     -H "X-API-KEY: $DATA_GOUV_API_KEY" -H "Content-Type: application/json" \
     -d '{"schema": {"name": "pass-culture-beneficiary", "version": "1.0.0",
          "url": "https://raw.githubusercontent.com/pass-culture/data-gcp/master/data_gouv/schemas/beneficiary/1.0.0/schema.json"}}'
   ```

6. About 15 minutes after the upload, check the preview of each resource and, under *Métadonnées > Extras*, that the analysis has no `error` (geographic columns should be detected).

## Change a resource

The `exp_data_gouv__*` models have an enforced dbt contract: adding, removing or retyping a column fails the build. A published schema version is immutable:

1. update the model and its `_schema` yml;
2. bump `SCHEMA_VERSION` in `build_schemas.py` (minor for an added column, major for a removed or renamed one), update the field definitions and run `python -I data_gouv/build_schemas.py`;
3. publish the new CSV and update the resource `schema` version and url on data.gouv.fr.
