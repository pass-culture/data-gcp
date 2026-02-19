import logging
import mimetypes
import uuid
from pathlib import PurePosixPath
from urllib.parse import urlparse

import httpx
import typer

from client import AllocineClient
from constants import (
    BQ_DATASET,
    GCP_PROJECT_ID,
    GCS_BUCKET,
    POSTER_PREFIX,
    RAW_TABLE,
    SECRET_ID,
    STAGING_TABLE,
)
from data import transform_movie
from gcp import (
    fetch_pending_posters,
    get_bq_client,
    get_secret,
    merge_staging_to_raw,
    truncate_and_load_staging,
    update_poster_failure,
    update_poster_success,
    upload_to_gcs,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

app = typer.Typer(help="Allocine ELT — sync movies and posters to BigQuery/GCS.")


@app.command("sync-movies")
def sync_movies() -> None:
    """Fetch all movies from Allocine API and sync to BigQuery."""
    api_key = get_secret(GCP_PROJECT_ID, SECRET_ID)
    bq = get_bq_client(GCP_PROJECT_ID)

    with AllocineClient(api_key=api_key) as client:
        raw_movies = client.fetch_all_movies()

    logger.info("Transforming %d movies...", len(raw_movies))
    transformed = [transform_movie(m) for m in raw_movies]

    logger.info("Loading %d rows into staging...", len(transformed))
    truncate_and_load_staging(transformed, GCP_PROJECT_ID, BQ_DATASET, STAGING_TABLE, bq_client=bq)

    logger.info("Merging staging into raw...")
    stats = merge_staging_to_raw(GCP_PROJECT_ID, BQ_DATASET, STAGING_TABLE, RAW_TABLE, bq_client=bq)

    logger.info(
        "sync-movies complete — %d fetched, %d new, %d updated, %d unchanged.",
        len(raw_movies),
        stats["inserted"],
        stats["updated"],
        len(raw_movies) - stats["inserted"] - stats["updated"],
    )


def _poster_uuid(poster_url: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, poster_url))


def _detect_extension(url: str, content_type: str | None) -> str:
    suffix = PurePosixPath(urlparse(url).path).suffix
    if suffix:
        return suffix.lstrip(".")
    if content_type:
        ext = mimetypes.guess_extension(content_type.split(";")[0].strip())
        if ext:
            return ext.lstrip(".")
    return "jpg"


@app.command("sync-posters")
def sync_posters(
    max_retries: int = typer.Option(3, help="Max download attempts per poster."),
) -> None:
    """Download pending posters from Allocine and upload to GCS."""
    from google.cloud import storage as gcs_lib

    bq = get_bq_client(GCP_PROJECT_ID)
    gcs_client = gcs_lib.Client()

    pending = fetch_pending_posters(GCP_PROJECT_ID, BQ_DATASET, RAW_TABLE, max_retries, bq_client=bq)
    logger.info("Found %d posters to download.", len(pending))

    with httpx.Client(timeout=60.0, follow_redirects=True) as http:
        for row in pending:
            movie_id: str = row["movie_id"]
            poster_url: str | None = row["poster_url"]

            try:
                response = http.get(poster_url)
                response.raise_for_status()

                content_type = response.headers.get("content-type")
                ext = _detect_extension(poster_url, content_type)
                uid = _poster_uuid(poster_url)
                blob_name = f"{POSTER_PREFIX}/{uid}.{ext}"

                gcs_uri = upload_to_gcs(
                    GCS_BUCKET,
                    blob_name,
                    response.content,
                    content_type=content_type or "image/jpeg",
                    gcs_client=gcs_client,
                )
                update_poster_success(movie_id, gcs_uri, GCP_PROJECT_ID, BQ_DATASET, RAW_TABLE, bq_client=bq)
                logger.info("movie_id=%s: uploaded → %s", movie_id, gcs_uri)

            except Exception as e:
                logger.error("movie_id=%s: failed (%s), incrementing retry.", movie_id, e)
                update_poster_failure(movie_id, GCP_PROJECT_ID, BQ_DATASET, RAW_TABLE, bq_client=bq)

    logger.info("sync-posters complete.")


if __name__ == "__main__":
    app()
