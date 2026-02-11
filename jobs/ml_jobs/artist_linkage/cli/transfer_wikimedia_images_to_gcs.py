import concurrent
import logging
import uuid
from concurrent.futures import ThreadPoolExecutor

import google.auth
import pandas as pd
import requests
import typer
from google.auth.transport.requests import AuthorizedSession
from google.cloud import storage
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from urllib3.util.retry import Retry

from src.constants import (
    ARTIST_MEDIATION_UUID_KEY,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
    WIKIDATA_IMAGE_FILE_URL_KEY,
    WIKIMEDIA_REQUEST_HEADER,
)

DE_DATALAKE_BUCKET_NAME = f"de-lake-{ENV_SHORT_NAME}"
DE_DATALAKE_IMAGES_FOLDER = "artist/images"
STATUS_KEY = "status"

# Parrallel download/upload settings. Increasing them can cause rate limiting issues.
MAX_WORKERS = 10
POOL_CONNECTIONS = MAX_WORKERS
POOL_MAXSIZE = MAX_WORKERS + 5


logging.basicConfig(level=logging.INFO)
app = typer.Typer()


def _get_session():
    """Create a requests session with retry strategy and connection pooling.

    Configures a session optimized for downloading images from Wikimedia with:
    - Automatic retries on transient failures (429, 5xx status codes)
    - Exponential backoff between retries
    - Connection pooling to reuse connections efficiently

    Returns:
        A configured requests.Session instance ready for making HTTP requests.
    """
    session = requests.Session()

    # Configure retry strategy
    retry_strategy = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )

    # Configure adapter with connection pooling
    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=POOL_CONNECTIONS,
        pool_maxsize=POOL_MAXSIZE,
    )

    session.mount("https://", adapter)

    return session


def _get_gcs_client():
    """Create a Google Cloud Storage client with authorized session.

    This client uses an AuthorizedSession with connection pooling, optimized for
    concurrent uploads/downloads to/from GCS.

    Returns:
        A google.cloud.storage.Client instance authenticated with default credentials.
    """
    credentials, _ = google.auth.default()
    authed_session = AuthorizedSession(credentials)
    adapter = HTTPAdapter(
        pool_connections=POOL_CONNECTIONS,
        pool_maxsize=POOL_MAXSIZE,
    )
    authed_session.mount("https://", adapter)

    client = storage.Client(
        project=GCP_PROJECT_ID, credentials=credentials, _http=authed_session
    )
    return client


def transfer_image(
    session: requests.Session, gcs_bucket: storage.Bucket, image_url: str
) -> dict:
    """Transfer an image from Wikimedia to Google Cloud Storage.

    Args:
        session: Requests session with connection pooling and retry configuration.
        gcs_bucket: GCS bucket where the image will be stored.
        image_url: URL of the image to download from Wikimedia.

    Returns:
        A dictionary containing the image URL, generated UUID, and transfer status.
        Possible statuses: SUCCESS, SKIPPED (if already exists), FAILED (with status code),
        or ERROR (with exception message).
    """
    try:
        # 1. Prepare GCS blob
        clean_url = image_url.strip()
        image_id = str(uuid.uuid5(uuid.NAMESPACE_URL, clean_url))
        blob = gcs_bucket.blob(f"{DE_DATALAKE_IMAGES_FOLDER}/{image_id}")

        # 2. Check if download is required
        if blob.exists():
            return {
                WIKIDATA_IMAGE_FILE_URL_KEY: image_url,
                ARTIST_MEDIATION_UUID_KEY: image_id,
                STATUS_KEY: "SKIPPED",
            }

        # 3. Stream wikiemedia content directly to GCS (no local save)
        with session.get(
            image_url, headers=WIKIMEDIA_REQUEST_HEADER, stream=True, timeout=15
        ) as r:
            if r.status_code == 200:
                blob.upload_from_file(r.raw, content_type=r.headers.get("content-type"))
                return {
                    WIKIDATA_IMAGE_FILE_URL_KEY: image_url,
                    ARTIST_MEDIATION_UUID_KEY: image_id,
                    STATUS_KEY: "SUCCESS",
                }
            else:
                return {
                    WIKIDATA_IMAGE_FILE_URL_KEY: image_url,
                    ARTIST_MEDIATION_UUID_KEY: None,
                    STATUS_KEY: "FAILED",
                }
    except Exception as e:
        logging.error(f"Error processing {image_url}: {e}")
        return {
            WIKIDATA_IMAGE_FILE_URL_KEY: image_url,
            ARTIST_MEDIATION_UUID_KEY: None,
            STATUS_KEY: "ERROR",
        }


def run_parallel_image_transfers(
    session: requests.Session, gcs_bucket: storage.Bucket, image_urls: list[str]
) -> pd.DataFrame:
    """Transfer multiple images from Wikimedia to GCS in parallel.

    Args:
        session: Requests session with connection pooling and retry configuration.
        gcs_bucket: GCS bucket where images will be stored.
        image_urls: List of image URLs to transfer.

    Returns:
        A DataFrame containing the transfer results with columns for image URL,
        UUID, and status for each image.
    """

    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(transfer_image, session, gcs_bucket, url): url
            for url in image_urls
        }

        for future in tqdm(
            concurrent.futures.as_completed(futures), total=len(futures)
        ):
            results.append(future.result())

    if len(results) == 0:
        logging.warning("No images were processed.")
        return pd.DataFrame(
            columns=[
                WIKIDATA_IMAGE_FILE_URL_KEY,
                ARTIST_MEDIATION_UUID_KEY,
                STATUS_KEY,
            ]
        )
    return pd.DataFrame(results)


@app.command()
def main(
    artists_matched_on_wikidata: str = typer.Option(),
    output_file_path: str = typer.Option(),
) -> None:
    """Transfer Wikimedia artist images to Google Cloud Storage.

    Reads a parquet file containing artist data with Wikidata matches,
    extracts unique image URLs, downloads them from Wikimedia, and uploads
    them to GCS. The results are merged back with the original data and
    saved to the output file.

    Args:
        artists_matched_on_wikidata: Path to the input parquet file containing
            artist data with Wikidata image URLs.
        output_file_path: Path where the output parquet file with transfer
            results will be saved.
    """
    # 1. Load Data
    artists_df = pd.read_parquet(artists_matched_on_wikidata)
    image_urls = artists_df[WIKIDATA_IMAGE_FILE_URL_KEY].dropna().unique().tolist()

    # 2. Setup sessions and clients
    session = _get_session()
    gcs_client = _get_gcs_client()
    bucket = gcs_client.bucket(DE_DATALAKE_BUCKET_NAME)

    # 3. Run transfers in parallel
    result_df = run_parallel_image_transfers(session, bucket, image_urls)

    # 4. Merge results and save output
    artists_df.merge(
        result_df, on=WIKIDATA_IMAGE_FILE_URL_KEY, how="left", validate="m:1"
    ).to_parquet(output_file_path)


if __name__ == "__main__":
    app()
