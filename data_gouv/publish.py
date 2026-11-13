"""Upload the data.gouv.fr resources and the dataset metadata through the API.

Creates the dataset as a draft (`private`) under the organization, or updates an
existing one (`--dataset`): title and description from `dataset_description.md`,
tags, licence, frequency, temporal coverage, then one resource per CSV of
`--csv-dir` with its Table Schema (raw `schema.json` on GitHub master). A
resource is matched by its schema name, so an update replaces the file of the
existing resource instead of adding a new one. The dataset is only made public
with `--publish`; a draft dataset is hidden, but its files stay downloadable by
anyone with their URL.

Files are sent in chunks with retries: data.gouv.fr drops some upload requests.

    export DATA_GOUV_API_KEY=...   # key of the target instance (demo and www differ)
    uv run python -I data_gouv/publish.py --env demo --csv-dir <dir>
    uv run python -I data_gouv/publish.py --env demo --csv-dir <dir> --dataset <id-or-slug>
    uv run python -I data_gouv/publish.py --env demo --dataset <id-or-slug> --publish
"""

import argparse
import csv
import hashlib
import json
import math
import os
import sys
import time
import uuid
from pathlib import Path

import requests

HERE = Path(__file__).resolve().parent
API = {
    "demo": "https://demo.data.gouv.fr/api/1",
    "www": "https://www.data.gouv.fr/api/1",
}
SCHEMA_VERSION = "1.0.0"
SCHEMA_URL = "https://raw.githubusercontent.com/pass-culture/data-gcp/master/data_gouv/schemas/{resource}/{version}/schema.json"
RESOURCES = [
    "beneficiary",
    "beneficiary_coverage",
    "diversity",
    "diversity_by_category",
]
TAGS = [
    "culture",
    "ministeredelaculture",
    "pass-culture",
    "jeunesse",
    "pratiques-culturelles",
]
LICENSE = "lov2"  # Licence Ouverte / Open Licence 2.0
FREQUENCY = "semiannual"
CHUNK_SIZE = 2_000_000


class Client:
    def __init__(self, base, key):
        self.base = base
        self.session = requests.Session()
        self.session.headers["X-API-KEY"] = key

    def call(self, method, path, retries=8, **kwargs):
        for attempt in range(1, retries + 1):
            try:
                r = self.session.request(
                    method, f"{self.base}{path}", timeout=300, **kwargs
                )
                if r.status_code < 500:
                    if not r.ok:
                        sys.exit(f"{method} {path}: {r.status_code} {r.text[:500]}")
                    return r.json() if r.content else None
                print(f"  {method} {path}: HTTP {r.status_code}, retry {attempt}")
            except (requests.ConnectionError, requests.Timeout):
                print(
                    f"  {method} {path}: connection dropped or timed out, retry {attempt}"
                )
            time.sleep(3 * attempt)
        sys.exit(f"{method} {path}: gave up after {retries} attempts")

    def upload(self, path, file):
        """Chunked upload: each part, then a request without `file` that assembles them.

        A file of a single part is sent in one request (the server only assembles
        uploads of more than one part).
        """
        size = file.stat().st_size
        parts = math.ceil(size / CHUNK_SIZE)
        if parts <= 1:
            with file.open("rb") as fh:
                return self.call(
                    "POST", path, files={"file": (file.name, fh.read(), "text/csv")}
                )
        upload_id = str(uuid.uuid4())
        with file.open("rb") as fh:
            for index in range(parts):
                data = fh.read(CHUNK_SIZE)
                self.call(
                    "POST",
                    path,
                    data={
                        "uuid": upload_id,
                        "filename": file.name,
                        "partindex": index,
                        "partbyteoffset": index * CHUNK_SIZE,
                        "totalfilesize": size,
                        "totalparts": parts,
                        "chunksize": len(data),
                    },
                    files={"file": (file.name, data, "text/csv")},
                )
                print(f"  {file.name}: part {index + 1}/{parts}")
        return self.call(
            "POST",
            path,
            data={
                "uuid": upload_id,
                "filename": file.name,
                "totalfilesize": size,
                "totalparts": parts,
            },
        )


def dataset_text():
    """Title (first heading) and description (the rest, without HTML comments)."""
    lines = (HERE / "dataset_description.md").read_text().splitlines()
    title = lines[0].removeprefix("# ").strip()
    body = [line for line in lines[1:] if not line.lstrip().startswith("<!--")]
    return title, "\n".join(body).strip() + "\n"


def month_range(files):
    """First and last month over the CSV files (first column, ISO dates)."""
    months = set()
    for file in files:
        with file.open(newline="") as fh:
            reader = csv.reader(fh)
            next(reader)
            months.update({row[0] for row in reader})
    return min(months), max(months)


def sha1(file):
    """Checksum data.gouv.fr stores for an uploaded file (change detection only)."""
    return hashlib.sha1(file.read_bytes(), usedforsecurity=False).hexdigest()


def schema(resource):
    content = json.loads(
        (HERE / "schemas" / resource / SCHEMA_VERSION / "schema.json").read_text()
    )
    return content, {
        "name": content["name"],
        "version": SCHEMA_VERSION,
        "url": SCHEMA_URL.format(resource=resource, version=SCHEMA_VERSION),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", choices=API, required=True)
    parser.add_argument(
        "--organization", default="pass-culture", help="organization slug or id"
    )
    parser.add_argument("--csv-dir", type=Path, help="CSV files from export_csv.py")
    parser.add_argument(
        "--dataset",
        help="id or slug of the dataset to update (default: create a draft)",
    )
    parser.add_argument(
        "--publish", action="store_true", help="make the dataset public"
    )
    args = parser.parse_args()
    if not args.csv_dir and not (args.dataset and args.publish):
        parser.error("--csv-dir is required, unless publishing an existing --dataset")

    key = os.environ.get("DATA_GOUV_API_KEY")
    if not key:
        sys.exit("DATA_GOUV_API_KEY is not set")
    api = Client(API[args.env], key)

    if args.csv_dir:
        files = {r: args.csv_dir / f"{r}.csv" for r in RESOURCES}
        missing = [str(f) for f in files.values() if not f.exists()]
        if missing:
            sys.exit(f"missing files: {missing}")
        title, description = dataset_text()
        start, end = month_range(files.values())
        metadata = {
            "title": title,
            "description": description,
            "tags": TAGS,
            "license": LICENSE,
            "frequency": FREQUENCY,
            "temporal_coverage": {"start": start, "end": end},
            "spatial": {"zones": ["country:fr"], "granularity": "fr:departement"},
        }
        if args.dataset:
            dataset = api.call("PUT", f"/datasets/{args.dataset}/", json=metadata)
        else:
            organization = api.call("GET", f"/organizations/{args.organization}/")
            dataset = api.call(
                "POST",
                "/datasets/",
                json={**metadata, "organization": organization["id"], "private": True},
            )
        dataset_id = dataset["id"]
        print(
            f"dataset {dataset['page']} (private={dataset['private']}), months {start} to {end}"
        )

        existing = {
            (r.get("schema") or {}).get("name"): r for r in dataset["resources"]
        }
        for resource, file in files.items():
            content, schema_ref = schema(resource)
            current = existing.get(schema_ref["name"])
            if current and (current.get("checksum") or {}).get("value") == sha1(file):
                uploaded = current
                print(f"  {file.name}: unchanged, not uploaded")
            elif current:
                uploaded = api.upload(
                    f"/datasets/{dataset_id}/resources/{current['id']}/upload/", file
                )
            else:
                uploaded = api.upload(f"/datasets/{dataset_id}/upload/", file)
            api.call(
                "PUT",
                f"/datasets/{dataset_id}/resources/{uploaded['id']}/",
                json={
                    "title": file.name,
                    "description": content["description"],
                    "schema": schema_ref,
                },
            )
            print(f"  {file.name}: {uploaded['url']}")

        # a retried request that had in fact succeeded leaves an extra resource
        expected = {schema(r)[1]["name"] for r in RESOURCES}
        resources = api.call("GET", f"/datasets/{dataset_id}/")["resources"]
        extra = [
            r for r in resources if (r.get("schema") or {}).get("name") not in expected
        ]
        for r in extra:
            print(
                f"  WARNING extra resource, check and delete it: {r['title']} {r['id']}"
            )
    else:
        dataset_id = args.dataset

    if args.publish:
        dataset = api.call("PUT", f"/datasets/{dataset_id}/", json={"private": False})
        print(f"published {dataset['page']} (private={dataset['private']})")


if __name__ == "__main__":
    main()
