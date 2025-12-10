"""Mode 4: Download images from BigQuery table URLs and upload to GCS."""

import json
from datetime import datetime

from google.cloud import bigquery, storage

from config import (
    DE_DATALAKE_BUCKET_NAME,
    DEFAULT_TARGET_TABLE,
    GCP_PROJECT_ID,
    IMAGE_DOWNLOAD_GCS_PREFIX,
    IMAGE_DOWNLOAD_MAX_WORKERS,
    IMAGE_DOWNLOAD_POOL_CONNECTIONS,
    IMAGE_DOWNLOAD_POOL_MAXSIZE,
    IMAGE_DOWNLOAD_SUB_BATCH_SIZE,
    IMAGE_DOWNLOAD_TIMEOUT,
)
from src.utils.bigquery import (
    count_failed_image_downloads,
    count_pending_image_downloads,
    fetch_batch_for_image_download,
    get_eans_not_in_product_table,
    get_images_status_breakdown,
    get_last_batch_number,
    get_sample_eans_by_images_status,
    get_status_breakdown,
    update_image_download_results,
)
from src.utils.image_download import (
    _get_session,
    batch_download_and_upload,
    calculate_url_uuid,
)
from src.utils.logging import get_logger

logger = get_logger(__name__)


def run_download_images(
    reprocess_failed: bool = False,
) -> None:
    """
    Download images from URLs in BigQuery table and upload to GCS.

    Unified architecture for both normal and reprocess modes:
    1. Iterate through batch_numbers (0, 1, 2, ...)
    2. For each batch_number:
       - Fetch ALL EANs for this batch (up to 20k) with matching status filter
       - Chunk into sub-batches of 1000 EANs
       - Process each sub-batch: threaded download + upload to GCS
       - Accumulate all results in memory
       - Write ALL results to BigQuery once when batch is complete
    3. Move to next batch_number

    Status filters:
    - Normal mode: images_download_status IS NULL (pending)
    - Reprocess mode: images_download_status = 'failed' (retry)

    All configuration loaded from config.py:
    - source_table: DEFAULT_TARGET_TABLE
    - gcs_bucket: DE_DATALAKE_BUCKET_NAME
    - gcs_prefix: IMAGE_DOWNLOAD_GCS_PREFIX
    - max_workers: IMAGE_DOWNLOAD_MAX_WORKERS
    - pool_connections: IMAGE_DOWNLOAD_POOL_CONNECTIONS
    - pool_maxsize: IMAGE_DOWNLOAD_POOL_MAXSIZE
    - timeout: IMAGE_DOWNLOAD_TIMEOUT
    - sub_batch_size: IMAGE_DOWNLOAD_SUB_BATCH_SIZE

    Args:
        reprocess_failed: If True, reprocess failed downloads; if False, process pending
    """
    source_table = DEFAULT_TARGET_TABLE
    gcs_bucket = DE_DATALAKE_BUCKET_NAME
    gcs_prefix = IMAGE_DOWNLOAD_GCS_PREFIX
    max_workers = IMAGE_DOWNLOAD_MAX_WORKERS
    pool_connections = IMAGE_DOWNLOAD_POOL_CONNECTIONS
    pool_maxsize = IMAGE_DOWNLOAD_POOL_MAXSIZE
    timeout = IMAGE_DOWNLOAD_TIMEOUT
    sub_batch_size = IMAGE_DOWNLOAD_SUB_BATCH_SIZE

    mode_label = "reprocess failed" if reprocess_failed else "normal"
    logger.info("=" * 70)
    logger.info("STARTING IMAGE DOWNLOAD")
    logger.info("=" * 70)
    logger.info(f"Mode: {mode_label}")
    logger.info(f"Source table: {source_table}")
    logger.info(f"Target: gs://{gcs_bucket}/{gcs_prefix}")
    logger.info(f"Max workers: {max_workers}, Timeout: {timeout}s")
    logger.info(f"Sub-batch size: {sub_batch_size} EANs")

    # Initialize clients
    bq_client = bigquery.Client(project=GCP_PROJECT_ID)
    storage_client = storage.Client(project=GCP_PROJECT_ID)
    session = _get_session(pool_connections, pool_maxsize, timeout)

    # CHECKPOINT 1: Initial state
    logger.info("-" * 70)
    logger.info("CHECKPOINT 1: INPUT STATE")
    logger.info("-" * 70)

    # Status breakdown
    status_breakdown = get_status_breakdown(bq_client, source_table)
    logger.info("EAN status breakdown:")
    for status, count in status_breakdown.items():
        logger.info(f"  - {status}: {count:,}")

    # Images status breakdown
    images_breakdown = get_images_status_breakdown(bq_client, source_table)
    logger.info("Images download status breakdown:")
    for status, count in images_breakdown.items():
        logger.info(f"  - {status}: {count:,}")

    # Get total count based on mode
    if reprocess_failed:
        total_count = count_failed_image_downloads(bq_client, source_table)
        mode_label = "failed"
    else:
        total_count = count_pending_image_downloads(bq_client, source_table)
        mode_label = "pending"

    logger.info(f"Total {mode_label} image downloads to process: {total_count:,}")

    if total_count == 0:
        logger.info(f"No {mode_label} image downloads. Exiting.")
        return

    # Get last batch number to know when to stop
    last_batch = get_last_batch_number(bq_client, source_table)
    logger.info(f"Last batch number in table: {last_batch}")

    # Start from batch 0 and iterate through all batches
    current_batch = 0
    total_processed = 0
    total_success = 0
    total_failed = 0

    # Process batches
    while current_batch <= last_batch:
        logger.info("-" * 70)
        logger.info(f"CHECKPOINT: BATCH {current_batch}")
        logger.info("-" * 70)

        # Fetch ALL EANs for this batch (up to 20k)
        all_rows = fetch_batch_for_image_download(
            bq_client, source_table, current_batch, reprocess_failed
        )

        if not all_rows:
            logger.info(
                f"No {mode_label} EANs in batch {current_batch}. Moving to next."
            )
            current_batch += 1
            continue

        logger.info(f"Fetched {len(all_rows)} {mode_label} EANs for processing")

        # Log EANs not in product_table (for debugging sync lag issues)
        not_in_product_count, sample_not_in_product = get_eans_not_in_product_table(
            bq_client, source_table, current_batch
        )
        if not_in_product_count > 0:
            logger.info(
                f"  [i] {not_in_product_count:,} EANs not yet in product_table "
                "(will still be processed)"
            )
            if sample_not_in_product:
                logger.info(f"    Sample: {sample_not_in_product}")

        # Check how many have old UUIDs (for change detection)
        eans_with_old_uuid = sum(
            1
            for r in all_rows
            if r.get("old_recto_image_uuid") or r.get("old_verso_image_uuid")
        )
        logger.info(
            f"  {eans_with_old_uuid:,} EANs have existing UUIDs (change detection)"
        )
        logger.info(
            f"  {len(all_rows) - eans_with_old_uuid:,} EANs are new (no old UUIDs)"
        )

        # Accumulate results for entire batch
        batch_results = []

        # Chunk into sub-batches and process
        for i in range(0, len(all_rows), sub_batch_size):
            sub_batch = all_rows[i : i + sub_batch_size]
            sub_batch_num = (i // sub_batch_size) + 1
            total_sub_batches = (len(all_rows) + sub_batch_size - 1) // sub_batch_size

            logger.info(
                f"  Sub-batch {sub_batch_num}/{total_sub_batches}: "
                f"Processing {len(sub_batch)} EANs"
            )

            # Process images for this sub-batch
            sub_batch_results = _process_batch_images(
                sub_batch,
                storage_client,
                session,
                gcs_bucket,
                gcs_prefix,
                max_workers,
                timeout,
            )

            # Accumulate results
            batch_results.extend(sub_batch_results)

            # Count sub-batch results
            sub_success = sum(
                1
                for r in sub_batch_results
                if r["images_download_status"] == "processed"
            )
            sub_failed = sum(
                1 for r in sub_batch_results if r["images_download_status"] == "failed"
            )

            logger.info(
                f"  Sub-batch {sub_batch_num}/{total_sub_batches} complete: "
                f"{sub_success} processed, {sub_failed} failed"
            )

        # Write ALL results for this batch_number to BigQuery once
        logger.info(
            f"Writing {len(batch_results)} results to BigQuery "
            f"for batch {current_batch}"
        )
        update_image_download_results(bq_client, source_table, batch_results)

        # Count batch results
        batch_success = sum(
            1 for r in batch_results if r["images_download_status"] == "processed"
        )
        batch_failed = sum(
            1 for r in batch_results if r["images_download_status"] == "failed"
        )

        logger.info(
            f"Batch {current_batch} complete: "
            f"{len(batch_results)} EANs processed "
            f"({batch_success} success, {batch_failed} failed)"
        )

        total_processed += len(batch_results)
        total_success += batch_success
        total_failed += batch_failed

        logger.info(
            f"Overall progress: {total_processed:,} EANs processed "
            f"({total_success:,} success, {total_failed:,} failed)"
        )

        # Move to next batch
        current_batch += 1

    # CHECKPOINT: FINAL VERIFICATION
    logger.info("-" * 70)
    logger.info("CHECKPOINT: FINAL VERIFICATION")
    logger.info("-" * 70)

    # Final images status breakdown
    final_images_breakdown = get_images_status_breakdown(bq_client, source_table)
    logger.info("Final images_download_status breakdown:")
    for status, count in final_images_breakdown.items():
        pct = count / sum(final_images_breakdown.values()) * 100
        logger.info(f"  - {status}: {count:,} ({pct:.1f}%)")

    # Check for unexpected NULL status
    null_count = final_images_breakdown.get("NULL", 0)
    if null_count > 0:
        logger.warning(f"⚠️ {null_count:,} EANs still have NULL images_download_status!")
        sample_null = get_sample_eans_by_images_status(bq_client, source_table, None, 5)
        logger.warning(f"  Sample NULL EANs: {sample_null}")
    else:
        logger.info("✓ All EANs have been processed (no NULL status remaining)")

    # Check for failed EANs
    failed_img_count = final_images_breakdown.get("failed", 0)
    if failed_img_count > 0:
        sample_failed = get_sample_eans_by_images_status(
            bq_client, source_table, "failed", 5
        )
        logger.warning(f"⚠️ {failed_img_count:,} EANs failed image download")
        logger.warning(f"  Sample failed EANs: {sample_failed}")

    # Summary
    logger.info("=" * 70)
    logger.info("IMAGE DOWNLOAD SUMMARY")
    logger.info("=" * 70)
    logger.info(f"Total processed: {total_processed:,}")
    logger.info(f"  - Success: {total_success:,}")
    logger.info(f"  - Failed: {total_failed:,}")
    no_change_total = total_processed - total_success - total_failed
    if no_change_total > 0:
        logger.info(f"  - No change: {no_change_total:,}")
    logger.info("=" * 70)


def _process_batch_images(
    rows: list[dict],
    storage_client: storage.Client,
    session,
    gcs_bucket: str,
    gcs_prefix: str,
    max_workers: int,
    timeout: int,
) -> list[dict]:
    """
    Process images for a batch of EANs using threaded download + upload.

    Extracts recto/verso URLs from json_raw, downloads both images with
    requests, and uploads to GCS with google.cloud.storage.

    Args:
        rows: List of dicts with keys: ean, json_raw
        storage_client: GCS storage client
        session: requests.Session for connection pooling
        gcs_bucket: GCS bucket name
        gcs_prefix: GCS path prefix
        max_workers: Maximum concurrent workers
        timeout: HTTP request timeout in seconds

    Returns:
        List of dicts with keys: ean, images_download_status,
        images_download_processed_at
    """
    # Extract image URLs and prepare download list
    download_tasks = []
    ean_to_images = {}  # Maps EAN to list of (url, gcs_path, image_type)
    ean_to_uuids = {}  # Maps EAN to dict with 'recto' and 'verso' UUIDs
    failed_eans = set()  # Track EANs that had parsing errors
    no_image_eans = set()  # Track EANs with ONLY "no_image" placeholder URLs
    no_image_url_count = 0  # Track total placeholder "no_image" URLs
    no_image_cache = {}  # In-memory cache: no_image URL -> UUID (minimizes GCS calls)

    # Filtering stats
    skipped_recto_count = 0  # image field != 1
    skipped_verso_count = 0  # image_4 field != 1
    unchanged_url_count = 0  # URL hash unchanged
    sample_skipped_recto = []
    sample_skipped_verso = []
    sample_parse_errors = []

    for row in rows:
        ean = row["ean"]
        json_raw = row["json_raw"]
        old_recto_uuid = row.get("old_recto_image_uuid")
        old_verso_uuid = row.get("old_verso_image_uuid")

        # Parse JSON to extract image URLs
        try:
            data = json.loads(json_raw)

            # Handle both article formats:
            # - List format: article = [{}]
            # - Dict format: article = {"1": {...}, "2": {...}}
            article_data = data.get("article")
            if isinstance(article_data, list):
                # List format - take first element
                article = article_data[0] if article_data else {}
            elif isinstance(article_data, dict):
                # Dict format - take first numeric key's value
                numeric_keys = sorted([k for k in article_data if k.isdigit()])
                article = article_data[numeric_keys[0]] if numeric_keys else {}
            else:
                article = {}

            images_url = article.get("imagesUrl", {})

            recto_url = images_url.get("recto")
            verso_url = images_url.get("verso")

            # Validate image availability based on image and image_4 fields
            image_field = article.get("image")
            image_4_field = article.get("image_4")

            if image_field != 1:
                skipped_recto_count += 1
                if len(sample_skipped_recto) < 3:
                    sample_skipped_recto.append((ean, image_field))
                recto_url = None

            if image_4_field != 1:
                skipped_verso_count += 1
                if len(sample_skipped_verso) < 3:
                    sample_skipped_verso.append((ean, image_4_field))
                verso_url = None

            ean_images = []
            ean_has_real_image = False
            ean_has_placeholder = False
            recto_uuid = None
            verso_uuid = None

            # Process recto with hash comparison
            is_placeholder, image_added, recto_uuid = _process_image_url(
                recto_url,
                "recto",
                gcs_bucket,
                gcs_prefix,
                download_tasks,
                ean_images,
                old_recto_uuid,
                storage_client,
                no_image_cache,
            )
            if is_placeholder:
                ean_has_placeholder = True
                no_image_url_count += 1
            if image_added:
                ean_has_real_image = True
            elif recto_url and not is_placeholder:
                unchanged_url_count += 1

            # Process verso with hash comparison
            is_placeholder, image_added, verso_uuid = _process_image_url(
                verso_url,
                "verso",
                gcs_bucket,
                gcs_prefix,
                download_tasks,
                ean_images,
                old_verso_uuid,
                storage_client,
                no_image_cache,
            )
            if is_placeholder:
                ean_has_placeholder = True
                no_image_url_count += 1
            if image_added:
                ean_has_real_image = True
            elif verso_url and not is_placeholder:
                unchanged_url_count += 1

            ean_to_images[ean] = ean_images
            ean_to_uuids[ean] = {"recto": recto_uuid, "verso": verso_uuid}

            # Track EANs with ONLY placeholder images (no real images)
            if ean_has_placeholder and not ean_has_real_image:
                no_image_eans.add(ean)

        except Exception as e:
            if len(sample_parse_errors) < 3:
                sample_parse_errors.append((ean, str(e)))
            ean_to_images[ean] = []
            ean_to_uuids[ean] = {"recto": None, "verso": None}
            failed_eans.add(ean)

    # Log filtering breakdown
    logger.info("  Filtering breakdown:")
    logger.info(f"    - Skipped recto (image!=1): {skipped_recto_count}")
    if sample_skipped_recto:
        logger.info(f"      Sample: {sample_skipped_recto}")
    logger.info(f"    - Skipped verso (image_4!=1): {skipped_verso_count}")
    if sample_skipped_verso:
        logger.info(f"      Sample: {sample_skipped_verso}")
    logger.info(f"    - Placeholder 'no_image' URLs: {no_image_url_count}")
    logger.info(f"    - Unchanged URLs (hash match): {unchanged_url_count}")
    logger.info(f"    - Parse errors: {len(failed_eans)}")
    if sample_parse_errors:
        logger.info(f"      Sample errors: {sample_parse_errors}")
    logger.info(f"  → Real images to download: {len(download_tasks)}")

    # Download all images using threaded approach
    if download_tasks:
        image_urls = [task[0] for task in download_tasks]
        gcs_paths = [task[1] for task in download_tasks]

        logger.info(f"Downloading {len(download_tasks)} images")

        # Run threaded download
        download_results = batch_download_and_upload(
            storage_client=storage_client,
            session=session,
            image_urls=image_urls,
            gcs_paths=gcs_paths,
            max_workers=max_workers,
            timeout=timeout,
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
        # Get UUIDs for this EAN
        uuids = ean_to_uuids.get(ean, {"recto": None, "verso": None})
        recto_uuid = uuids.get("recto")
        verso_uuid = uuids.get("verso")

        # Check if this EAN had a parsing error
        if ean in failed_eans:
            # Parsing failed - mark as failed
            results.append(
                {
                    "ean": ean,
                    "images_download_status": "failed",
                    "images_download_processed_at": current_time,
                    "recto_image_uuid": recto_uuid,
                    "verso_image_uuid": verso_uuid,
                }
            )
            continue

        # Check if this EAN has only placeholder images
        if ean in no_image_eans:
            # Only placeholder images - mark as processed without download
            results.append(
                {
                    "ean": ean,
                    "images_download_status": "processed",
                    "images_download_processed_at": current_time,
                    "recto_image_uuid": recto_uuid,
                    "verso_image_uuid": verso_uuid,
                }
            )
            continue

        if not images:
            # No images to download (all skipped due to unchanged URLs or no URLs)
            # Mark as 'no_change' - URLs haven't changed
            results.append(
                {
                    "ean": ean,
                    "images_download_status": "no_change",
                    "images_download_processed_at": current_time,
                    "recto_image_uuid": recto_uuid,
                    "verso_image_uuid": verso_uuid,
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
                    "recto_image_uuid": recto_uuid,
                    "verso_image_uuid": verso_uuid,
                }
            )
        else:
            results.append(
                {
                    "ean": ean,
                    "images_download_status": "failed",
                    "images_download_processed_at": current_time,
                    "recto_image_uuid": recto_uuid,
                    "verso_image_uuid": verso_uuid,
                }
            )

    # Log final summary
    processed_count = sum(
        1 for r in results if r["images_download_status"] == "processed"
    )
    failed_count = sum(1 for r in results if r["images_download_status"] == "failed")
    no_change_count = sum(
        1 for r in results if r["images_download_status"] == "no_change"
    )

    logger.info(
        f"Batch complete: {processed_count} EANs processed, "
        f"{no_change_count} no change, "
        f"{failed_count} failed "
        f"({len(no_image_eans)} with placeholders only)"
    )

    return results


def _process_image_url(
    image_url: str | None,
    image_type: str,
    gcs_bucket: str,
    gcs_prefix: str,
    download_tasks: list,
    ean_images: list,
    old_uuid: str | None = None,
    storage_client: storage.Client | None = None,
    no_image_cache: dict[str, str] | None = None,
) -> tuple[bool, bool, str | None]:
    """
    Process a single image URL (recto or verso) and add to download tasks if URL changed

    Handles placeholder "no_image" URLs with caching and GCS existence checks:
    - First encounter: checks cache, then GCS bucket, downloads if needed
    - Subsequent encounters: instant cache hit (zero I/O)

    Uses UUID5 hash-based change detection:
    - Calculates new UUID from URL (deterministic)
    - Compares with old UUID
    - Only downloads if UUID changed

    Args:
        image_url: URL of the image to download (None if not present)
        image_type: Type of image ("recto" or "verso")
        gcs_bucket: GCS bucket name
        gcs_prefix: GCS path prefix
        download_tasks: List to append download task to (modified in place)
        ean_images: List to append image info to (modified in place)
        old_uuid: Previous UUID from product_mediation (None if first time)
        storage_client: GCS client for checking blob existence
        no_image_cache: In-memory cache mapping no_image URL -> UUID

    Returns:
        Tuple of (is_placeholder: bool, image_added: bool, uuid: str | None)
        - is_placeholder: True if a "no_image" URL was found
        - image_added: True if a real image was added to download tasks
        - uuid: UUID5 of the image URL (None if no URL)
    """
    if image_url:
        # Check if placeholder "no_image" URL
        if "no_image" in image_url.lower():
            # Check in-memory cache first (zero I/O)
            if no_image_cache is not None and image_url in no_image_cache:
                cached_uuid = no_image_cache[image_url]
                return (True, False, cached_uuid)

            # First time seeing this no_image URL - calculate UUID
            uuid = calculate_url_uuid(image_url)

            # Check if blob exists in GCS (one call per unique no_image URL)
            if storage_client is not None:
                bucket = storage_client.bucket(gcs_bucket)
                blob_path = f"{gcs_prefix}/{uuid}"
                blob = bucket.blob(blob_path)

                if not blob.exists():
                    # Blob doesn't exist - add to download tasks
                    gcs_path = f"gs://{gcs_bucket}/{blob_path}"
                    download_tasks.append((image_url, gcs_path))

            # Cache the UUID for subsequent EANs
            if no_image_cache is not None:
                no_image_cache[image_url] = uuid

            return (True, False, uuid)  # is_placeholder=True, image_added=False

        # Calculate deterministic UUID5 from URL
        new_uuid = calculate_url_uuid(image_url)

        # Compare with old UUID - only download if changed
        if new_uuid != old_uuid:
            # URL changed - need to download
            gcs_path = f"gs://{gcs_bucket}/{gcs_prefix}/{new_uuid}"

            download_tasks.append((image_url, gcs_path))
            ean_images.append(
                {"url": image_url, "gcs_path": gcs_path, "type": image_type}
            )
            return (
                False,
                True,
                new_uuid,
            )  # is_placeholder=False, image_added=True, uuid=new_uuid
        else:
            # URL unchanged - skip download
            return (
                False,
                False,
                new_uuid,
            )  # is_placeholder=False, image_added=False, uuid=new_uuid

    return (False, False, None)  # No URL present
