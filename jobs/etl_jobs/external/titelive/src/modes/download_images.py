"""Mode 4: Download images from BigQuery table URLs and upload to GCS."""

import asyncio
import json
import uuid
from datetime import datetime

from google.cloud import bigquery, storage

from src.constants import GCP_PROJECT_ID
from src.utils.bigquery import (
    count_failed_image_downloads,
    count_pending_image_downloads,
    fetch_batch_for_image_download,
    fetch_failed_image_downloads,
    get_last_batch_number,
    update_image_download_results,
)
from src.utils.image_download import batch_download_and_upload
from src.utils.logging import get_logger

logger = get_logger(__name__)


def run_download_images(
    source_table: str,
    gcs_bucket: str,
    gcs_prefix: str,
    max_concurrent: int = 100,
    batch_size: int = 1000,
    reprocess_failed: bool = False,
) -> None:
    """
    Download images from URLs in BigQuery source table and upload to GCS.

    Extracts recto and verso image URLs from json_raw column and downloads them.
    Uses batch_number from source table for progress tracking.

    Architecture:
    1. Iterate through all batches starting from batch 0
    2. For each batch N:
       - Fetch EANs WHERE batch_number = N AND images_download_status IS NULL
       - Extract recto/verso URLs from json_raw
       - Download and upload to GCS
       - Update images_download_status: 'processed' or 'failed'
    3. Automatically resumes by skipping already-processed EANs
    (images_download_status IS NULL)

    Optional: Reprocess failed downloads
    - If reprocess_failed=True, fetches EANs with images_download_status='failed'
    - Reprocesses and updates status

    Args:
        source_table: Source BigQuery table with batch_number (project.dataset.table)
        gcs_bucket: Target GCS bucket name (without gs://)
        gcs_prefix: Prefix for GCS paths (e.g., 'images/titelive')
        max_concurrent: Maximum concurrent downloads (default: 100)
        batch_size: Number of EANs to process per batch (default: 1000)
        reprocess_failed: If True, reprocess failed downloads (default: False)
    """
    logger.info(f"Starting image download from {source_table}")
    logger.info(f"Target: gs://{gcs_bucket}/{gcs_prefix}")
    logger.info(f"Max concurrent downloads: {max_concurrent}")
    logger.info(f"Batch size: {batch_size}")
    logger.info(f"Reprocess failed mode: {reprocess_failed}")

    # Initialize clients
    bq_client = bigquery.Client(project=GCP_PROJECT_ID)
    storage_client = storage.Client(project=GCP_PROJECT_ID)

    if reprocess_failed:
        # Reprocess failed downloads mode
        _run_reprocess_failed_mode(
            bq_client,
            storage_client,
            source_table,
            gcs_bucket,
            gcs_prefix,
            max_concurrent,
            batch_size,
        )
    else:
        # Normal mode (automatically resumes via images_download_status IS NULL)
        _run_batch_mode(
            bq_client,
            storage_client,
            source_table,
            gcs_bucket,
            gcs_prefix,
            max_concurrent,
        )


def _run_batch_mode(
    bq_client: bigquery.Client,
    storage_client: storage.Client,
    source_table: str,
    gcs_bucket: str,
    gcs_prefix: str,
    max_concurrent: int,
) -> None:
    """
    Process images batch by batch using batch_number.

    Automatically resumes by filtering WHERE images_download_status IS NULL.

    Args:
        bq_client: BigQuery client
        storage_client: GCS client
        source_table: Source table with batch_number
        gcs_bucket: GCS bucket name
        gcs_prefix: GCS path prefix
        max_concurrent: Max concurrent downloads
    """
    # Get total counts
    total_pending = count_pending_image_downloads(bq_client, source_table)
    logger.info(f"Total pending image downloads: {total_pending:,}")

    if total_pending == 0:
        logger.info("No pending image downloads. Exiting.")
        return

    # Start from batch 0 and iterate through all batches
    current_batch = 0
    logger.info("Starting from batch 0")

    total_processed = 0
    total_success = 0
    total_failed = 0

    # Process batches
    while True:
        logger.info(f"Processing batch {current_batch}")

        # Fetch EANs for this batch
        rows = fetch_batch_for_image_download(bq_client, source_table, current_batch)

        if not rows:
            logger.info(
                f"No more EANs to process in batch {current_batch}. "
                "Moving to next batch."
            )

            # Check if there are any more pending downloads in higher batches
            last_batch = get_last_batch_number(bq_client, source_table)
            if current_batch >= last_batch:
                logger.info("Reached last batch. Exiting.")
                break

            current_batch += 1
            continue

        logger.info(
            f"Batch {current_batch}: Processing {len(rows)} EANs with image downloads"
        )

        # Process images for this batch
        batch_results = _process_batch_images(
            rows, storage_client, gcs_bucket, gcs_prefix, max_concurrent
        )

        # Count results
        batch_success = sum(
            1 for r in batch_results if r["images_download_status"] == "processed"
        )
        batch_failed = sum(
            1 for r in batch_results if r["images_download_status"] == "failed"
        )

        logger.info(
            f"Batch {current_batch} complete: "
            f"{batch_success} processed, {batch_failed} failed"
        )

        # Update BigQuery with results
        update_image_download_results(bq_client, source_table, batch_results)

        total_processed += len(rows)
        total_success += batch_success
        total_failed += batch_failed

        logger.info(
            f"Progress: {total_processed}/{total_pending} EANs processed "
            f"({total_success} success, {total_failed} failed)"
        )

        # Move to next batch
        current_batch += 1

    logger.info(
        f"Image download complete: {total_processed:,} EANs processed, "
        f"{total_success:,} success, {total_failed:,} failed"
    )


def _run_reprocess_failed_mode(
    bq_client: bigquery.Client,
    storage_client: storage.Client,
    source_table: str,
    gcs_bucket: str,
    gcs_prefix: str,
    max_concurrent: int,
    batch_size: int,
) -> None:
    """
    Reprocess failed image downloads.

    Args:
        bq_client: BigQuery client
        storage_client: GCS client
        source_table: Source table
        gcs_bucket: GCS bucket name
        gcs_prefix: GCS path prefix
        max_concurrent: Max concurrent downloads
        batch_size: Number of EANs per batch
    """
    # Get total failed count
    total_failed = count_failed_image_downloads(bq_client, source_table)
    logger.info(f"Total failed image downloads to reprocess: {total_failed:,}")

    if total_failed == 0:
        logger.info("No failed image downloads to reprocess. Exiting.")
        return

    total_processed = 0
    total_success = 0
    total_still_failed = 0

    # Process in batches
    while True:
        # Fetch failed records
        rows = fetch_failed_image_downloads(bq_client, source_table, batch_size)

        if not rows:
            logger.info("No more failed image downloads to reprocess. Exiting.")
            break

        logger.info(f"Reprocessing {len(rows)} failed EANs")

        # Process images
        batch_results = _process_batch_images(
            rows, storage_client, gcs_bucket, gcs_prefix, max_concurrent
        )

        # Count results
        batch_success = sum(
            1 for r in batch_results if r["images_download_status"] == "processed"
        )
        batch_failed = sum(
            1 for r in batch_results if r["images_download_status"] == "failed"
        )

        logger.info(
            f"Reprocess batch complete: {batch_success} now processed, "
            f"{batch_failed} still failed"
        )

        # Update BigQuery with results
        update_image_download_results(bq_client, source_table, batch_results)

        total_processed += len(rows)
        total_success += batch_success
        total_still_failed += batch_failed

        logger.info(
            f"Progress: {total_processed}/{total_failed} EANs reprocessed "
            f"({total_success} now success, {total_still_failed} still failed)"
        )

    logger.info(
        f"Reprocess complete: {total_processed:,} EANs reprocessed, "
        f"{total_success:,} now success, {total_still_failed:,} still failed"
    )


def _process_batch_images(
    rows: list[dict],
    storage_client: storage.Client,
    gcs_bucket: str,
    gcs_prefix: str,
    max_concurrent: int,
) -> list[dict]:
    """
    Process images for a batch of EANs.

    Extracts recto/verso URLs from json_raw, downloads both images,
    and returns results with status.

    Args:
        rows: List of dicts with keys: ean, json_raw
        storage_client: GCS client instance
        gcs_bucket: GCS bucket name
        gcs_prefix: GCS path prefix
        max_concurrent: Maximum concurrent downloads

    Returns:
        List of dicts with keys: ean, images_download_status,
        images_download_processed_at
    """
    # Extract image URLs and prepare download list
    download_tasks = []
    ean_to_images = {}  # Maps EAN to list of (url, gcs_path, image_type)

    for row in rows:
        ean = row["ean"]
        json_raw = row["json_raw"]

        # Parse JSON to extract image URLs
        try:
            data = json.loads(json_raw)
            article = data.get("article", [{}])[0]
            images_url = article.get("imagesUrl", {})

            recto_url = images_url.get("recto")
            verso_url = images_url.get("verso")

            ean_images = []

            # Add recto if present
            if recto_url:
                recto_id = str(uuid.uuid4())
                recto_extension = _extract_extension(recto_url)
                recto_gcs_path = (
                    f"gs://{gcs_bucket}/{gcs_prefix}/{recto_id}{recto_extension}"
                )

                download_tasks.append((recto_url, recto_gcs_path))
                ean_images.append(
                    {"url": recto_url, "gcs_path": recto_gcs_path, "type": "recto"}
                )

            # Add verso if present
            if verso_url:
                verso_id = str(uuid.uuid4())
                verso_extension = _extract_extension(verso_url)
                verso_gcs_path = (
                    f"gs://{gcs_bucket}/{gcs_prefix}/{verso_id}{verso_extension}"
                )

                download_tasks.append((verso_url, verso_gcs_path))
                ean_images.append(
                    {"url": verso_url, "gcs_path": verso_gcs_path, "type": "verso"}
                )

            ean_to_images[ean] = ean_images

        except Exception as e:
            logger.error(f"Failed to parse json_raw for EAN {ean}: {e}")
            ean_to_images[ean] = []

    # Download all images
    if download_tasks:
        image_urls = [task[0] for task in download_tasks]
        gcs_paths = [task[1] for task in download_tasks]

        logger.info(f"Downloading {len(download_tasks)} images ({len(rows)} EANs)")

        download_results = asyncio.run(
            batch_download_and_upload(
                image_urls=image_urls,
                gcs_paths=gcs_paths,
                storage_client=storage_client,
                max_concurrent=max_concurrent,
            )
        )

        # Map results back to URLs
        url_to_result = {
            url: (success, message) for success, url, message in download_results
        }
    else:
        url_to_result = {}

    # Build final results per EAN
    results = []
    current_time = datetime.now()

    for ean, images in ean_to_images.items():
        if not images:
            # No images found for this EAN - mark as processed
            results.append(
                {
                    "ean": ean,
                    "images_download_status": "processed",
                    "images_download_processed_at": current_time,
                }
            )
            continue

        # Check if all images succeeded
        all_success = True
        for img in images:
            success, _ = url_to_result.get(img["url"], (False, "Unknown error"))
            if not success:
                all_success = False
                break

        if all_success:
            results.append(
                {
                    "ean": ean,
                    "images_download_status": "processed",
                    "images_download_processed_at": current_time,
                }
            )
        else:
            results.append(
                {
                    "ean": ean,
                    "images_download_status": "failed",
                    "images_download_processed_at": current_time,
                }
            )

    return results


def _extract_extension(url: str) -> str:
    """
    Extract file extension from URL.

    Args:
        url: Image URL

    Returns:
        Extension with dot (e.g., '.jpg') or empty string
    """
    # Remove query parameters
    url_path = url.split("?")[0]

    # Get extension
    if "." in url_path:
        extension = url_path.rsplit(".", 1)[-1].lower()
        # Only keep common image extensions
        if extension in ["jpg", "jpeg", "png", "gif", "webp", "bmp"]:
            return f".{extension}"

    return ".jpg"  # Default extension
