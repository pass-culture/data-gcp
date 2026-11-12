"""Export the data.gouv.fr resources from BigQuery to local CSV files.

One CSV per resource (`<resource>.csv`), UTF-8, comma separated, header row,
ISO dates, lowercase booleans and empty cells for missing values, as expected by
data.gouv.fr (csv-detective) and the Table Schemas in `data_gouv/schemas`.

    uv run python -I data_gouv/export_csv.py --out-dir <dir>
    # before the tables exist, from the dbt compiled SQL:
    uv run python -I data_gouv/export_csv.py --out-dir <dir> \
        --compiled-dir orchestration/dags/data_gcp_dbt/target/compiled/data_gcp_dbt/models/export/data_gouv
"""

import argparse
import csv
import datetime
from pathlib import Path

from google.cloud import bigquery

RESOURCES = [
    "beneficiary",
    "beneficiary_coverage",
    "diversity",
    "diversity_by_category",
]


def format_value(value):
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, datetime.date):
        return value.isoformat()
    return value


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", required=True, type=Path)
    parser.add_argument("--project", default="passculture-data-prod")
    parser.add_argument("--dataset", default="export_data_gouv_prod")
    parser.add_argument("--compiled-dir", type=Path)
    args = parser.parse_args()

    client = bigquery.Client(project=args.project)
    args.out_dir.mkdir(parents=True, exist_ok=True)
    for resource in RESOURCES:
        if args.compiled_dir:
            source = f"({(args.compiled_dir / f'exp_data_gouv__{resource}.sql').read_text()})"
        else:
            source = f"`{args.project}.{args.dataset}.{resource}`"
        rows = client.query(f"select * from {source} order by 1, 2, 3, 4, 5").result()
        path = args.out_dir / f"{resource}.csv"
        with path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f, lineterminator="\n")
            writer.writerow([field.name for field in rows.schema])
            n = 0
            for row in rows:
                writer.writerow([format_value(v) for v in row.values()])
                n += 1
        print(f"{path}: {n} rows, {path.stat().st_size / 1e6:.1f} MB")


if __name__ == "__main__":
    main()
