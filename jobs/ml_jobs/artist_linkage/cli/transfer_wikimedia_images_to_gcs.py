import concurrent
import logging
import uuid

import pandas as pd
import requests
import typer
from google.cloud import storage

from src.constants import (
    ARTIST_MEDIATION_UUID_KEY,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
    IMAGE_FILE_URL_KEY,
    WIKIMEDIA_REQUEST_HEADER,
)

MAX_WORKERS = 10
DE_DATALAKE_BUCKET_NAME = f"de-lake-{ENV_SHORT_NAME}"
DE_DATALAKE_IMAGES_FOLDER = "artist/images"
STATUS_KEY = "status"

logging.basicConfig(level=logging.INFO)
app = typer.Typer()


def transfer_image(gcs_bucket: storage.Bucket, image_url: str) -> dict:
    """Downloads an image stream and uploads it directly to GCS."""
    # 1. Open connection to Wikimedia
    with requests.get(
        image_url, headers=WIKIMEDIA_REQUEST_HEADER, stream=True, timeout=15
    ) as r:
        if r.status_code == 200:
            # 2. Create a new blob in GCS
            clean_url = image_url.strip()
            image_id = str(uuid.uuid5(uuid.NAMESPACE_URL, clean_url))
            blob = gcs_bucket.blob(f"{DE_DATALAKE_IMAGES_FOLDER}/{image_id}")

            if blob.exists():
                return {
                    IMAGE_FILE_URL_KEY: image_url,
                    ARTIST_MEDIATION_UUID_KEY: image_id,
                    STATUS_KEY: f"SKIPPED - {image_url} already exists as {image_id}",
                }

            # 3. Stream content directly to GCS (no local save)
            # content_type helps GCS serve it correctly in the browser later
            blob.upload_from_file(r.raw, content_type=r.headers.get("content-type"))
            return {
                IMAGE_FILE_URL_KEY: image_url,
                ARTIST_MEDIATION_UUID_KEY: image_id,
                STATUS_KEY: "SUCCESS",
            }
        else:
            return {
                IMAGE_FILE_URL_KEY: image_url,
                STATUS_KEY: f"FAILED ({r.status_code})",
            }


def run_parallel_image_transfers(
    gcs_bucket: storage.Bucket, image_urls: list[str]
) -> pd.DataFrame:
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(transfer_image, gcs_bucket, url): url for url in image_urls
        }

        for future in concurrent.futures.as_completed(futures):
            results.append(future.result())
    return pd.DataFrame(results)


@app.command()
def main(
    artists_matched_on_wikidata: str = typer.Option(),
    output_file_path: str = typer.Option(),
) -> None:
    artists_df = pd.read_parquet(artists_matched_on_wikidata)

    client = storage.Client(GCP_PROJECT_ID)
    bucket = client.bucket(DE_DATALAKE_BUCKET_NAME)
    image_urls = artists_df.image_file_url.dropna().tolist()

    result_df = run_parallel_image_transfers(bucket, image_urls)

    artists_df.merge(
        result_df,
        on=IMAGE_FILE_URL_KEY,
        how="left",
    ).to_parquet(output_file_path)


if __name__ == "__main__":
    app()
