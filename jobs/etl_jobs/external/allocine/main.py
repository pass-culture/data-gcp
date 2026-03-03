import logging

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
from gcp import get_bq_client, get_secret, upload_to_gcs
from load import (
    fetch_pending_posters,
    merge_staging_to_raw,
    poster_blob_name,
    truncate_and_load_staging,
    update_poster_failure,
    update_poster_success,
)
from transform import transform_movie

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
            poster_url: str = row["poster_url"]

            try:
                response = http.get(poster_url)
                response.raise_for_status()

                content_type = response.headers.get("content-type")
                blob_name = poster_blob_name(POSTER_PREFIX, poster_url, content_type)

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
