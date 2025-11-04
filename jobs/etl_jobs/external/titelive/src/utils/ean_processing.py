"""Shared utilities for processing EANs via Titelive API."""

import requests

from config import DEFAULT_BATCH_SIZE, MUSIC_SUBCATEGORIES, TiteliveCategory
from src.api.client import TiteliveClient
from src.utils.api_transform import transform_api_response
from src.utils.logging import get_logger

logger = get_logger(__name__)


def process_eans_batch(
    api_client: TiteliveClient,
    ean_pairs: list[tuple[str, str]],
    sub_batch_size: int = DEFAULT_BATCH_SIZE,
) -> list[dict]:
    """
    Process a batch of EANs by calling the API in sub-batches, grouped by base.

    Groups EANs by base (music vs paper) based on subcategoryid, then processes
    each group in sub-batches of sub_batch_size (default 250).
    For each sub-batch:
    - Call API with EANs and appropriate base parameter
    - Mark returned EANs as 'processed'
    - Mark missing EANs as 'deleted_in_titelive'
    - Mark failed API calls as 'failed'

    Args:
        api_client: Titelive API client
        ean_pairs: List of (ean, subcategoryid) tuples to process
        sub_batch_size: Number of EANs per API call (default 250)

    Returns:
        List of dicts with keys: ean, subcategoryid, status, json_raw
        Status values: 'processed' | 'deleted_in_titelive' | 'failed'
    """
    # Group EANs by base category
    music_eans = []
    paper_eans = []

    for ean, subcategoryid in ean_pairs:
        if subcategoryid in MUSIC_SUBCATEGORIES:
            music_eans.append((ean, subcategoryid))
        else:
            # Default to paper for NULL, unknown, or paper subcategories
            paper_eans.append((ean, subcategoryid))

    total_eans = len(ean_pairs)
    logger.info(
        f"Processing {total_eans} EANs: "
        f"{len(music_eans)} music, {len(paper_eans)} paper"
    )

    results = []

    # Process music EANs
    if music_eans:
        logger.info(f"Processing {len(music_eans)} music EANs")
        results.extend(
            _process_eans_by_base(
                api_client, music_eans, TiteliveCategory.MUSIC, sub_batch_size
            )
        )

    # Process paper EANs
    if paper_eans:
        logger.info(f"Processing {len(paper_eans)} paper EANs")
        results.extend(
            _process_eans_by_base(
                api_client, paper_eans, TiteliveCategory.PAPER, sub_batch_size
            )
        )

    return results


def _process_eans_by_base(
    api_client: TiteliveClient,
    ean_pairs: list[tuple[str, str]],
    base: str,
    sub_batch_size: int,
) -> list[dict]:
    """
    Process EANs for a specific base category in sub-batches.

    Args:
        api_client: Titelive API client
        ean_pairs: List of (ean, subcategoryid) tuples
        base: API base category ('music' or 'paper')
        sub_batch_size: Number of EANs per API call

    Returns:
        List of dicts with processing results
    """
    results = []
    total_eans = len(ean_pairs)
    total_sub_batches = (total_eans + sub_batch_size - 1) // sub_batch_size

    logger.info(
        f"Processing {total_eans} {base} EANs in {total_sub_batches} sub-batches "
        f"of {sub_batch_size} EANs each"
    )

    for i in range(0, len(ean_pairs), sub_batch_size):
        sub_batch_pairs = ean_pairs[i : i + sub_batch_size]
        sub_batch_eans = [ean for ean, _ in sub_batch_pairs]
        current_sub_batch = (i // sub_batch_size) + 1
        eans_processed_so_far = i
        eans_in_this_batch = len(sub_batch_pairs)

        logger.info(
            f"Sub-batch {current_sub_batch}/{total_sub_batches} ({base}): "
            f"Processing EANs {eans_processed_so_far + 1}-"
            f"{eans_processed_so_far + eans_in_this_batch} "
            f"of {total_eans}"
        )

        try:
            # Call API with base parameter
            api_response = api_client.get_by_eans_with_base(sub_batch_eans, base)

            # Transform response
            transformed_df = transform_api_response(api_response)
            # logger.info(f"Transformed {transformed_df.head(1)} rows")

            # Identify returned vs missing EANs
            returned_eans = (
                set(transformed_df["ean"].tolist())
                if not transformed_df.empty
                else set()
            )
            missing_eans = set(sub_batch_eans) - returned_eans

            # Create mapping of ean to subcategoryid for this sub-batch
            ean_to_subcategoryid = dict(sub_batch_pairs)

            # Add processed results
            for _, row in transformed_df.iterrows():
                results.append(
                    {
                        "ean": row["ean"],
                        "subcategoryid": ean_to_subcategoryid.get(row["ean"]),
                        "status": "processed",
                        "json_raw": row["json_raw"],
                    }
                )

            # Add deleted results
            for ean in missing_eans:
                results.append(
                    {
                        "ean": ean,
                        "subcategoryid": ean_to_subcategoryid.get(ean),
                        "status": "deleted_in_titelive",
                        "json_raw": None,
                    }
                )

            logger.info(
                f"Sub-batch {current_sub_batch}/{total_sub_batches} ({base}) complete: "
                f"{len(returned_eans)} processed, "
                f"{len(missing_eans)} deleted | "
                "Progress: "
                f"{eans_processed_so_far + eans_in_this_batch}/{total_eans} EANs"
            )

        except requests.exceptions.HTTPError as e:
            # Handle 404 errors by processing EANs individually
            if e.response.status_code == 404:
                # Check if we're already processing individual EANs (batch size 1)
                if sub_batch_size == 1:
                    # Single EAN returned 404 - mark as deleted_in_titelive
                    logger.info(
                        f"EAN {sub_batch_pairs[0][0]} not found (404) - \
                        marking as deleted_in_titelive"
                    )
                    for ean, subcategoryid in sub_batch_pairs:
                        results.append(
                            {
                                "ean": ean,
                                "subcategoryid": subcategoryid,
                                "status": "deleted_in_titelive",
                                "json_raw": None,
                            }
                        )
                else:
                    # Batch of EANs returned 404 - process each individually
                    # to identify them
                    logger.warning(
                        f"404 error for sub-batch "
                        f"{current_sub_batch}/{total_sub_batches} ({base}). "
                        f"Processing {len(sub_batch_pairs)} EANs individually "
                        "to identify problematic EAN(s)"
                    )
                    # Process each EAN individually by calling this function
                    # recursively with batch size 1
                    individual_results = _process_eans_by_base(
                        api_client, sub_batch_pairs, base, sub_batch_size=1
                    )
                    results.extend(individual_results)
                    logger.info(
                        f"Individual processing complete for sub-batch "
                        f"{current_sub_batch}/{total_sub_batches} ({base}) | "
                        "Progress: "
                        f"{eans_processed_so_far + eans_in_this_batch}/{total_eans} "
                        "EANs"
                    )
            else:
                # Mark all EANs in sub-batch as failed for other HTTP errors
                logger.error(
                    f"HTTP {e.response.status_code} error for sub-batch "
                    f"{current_sub_batch}/{total_sub_batches} ({base}): {e}"
                )
                for ean, subcategoryid in sub_batch_pairs:
                    results.append(
                        {
                            "ean": ean,
                            "subcategoryid": subcategoryid,
                            "status": "failed",
                            "json_raw": None,
                        }
                    )
                logger.warning(
                    f"Marked {len(sub_batch_pairs)} EANs as failed | "
                    "Progress: "
                    f"{eans_processed_so_far + eans_in_this_batch}/{total_eans} EANs"
                )

        except Exception as e:
            # Mark all EANs in sub-batch as failed for non-HTTP exceptions
            logger.error(
                f"API call failed for sub-batch "
                f"{current_sub_batch}/{total_sub_batches} ({base}): {e}"
            )
            for ean, subcategoryid in sub_batch_pairs:
                results.append(
                    {
                        "ean": ean,
                        "subcategoryid": subcategoryid,
                        "status": "failed",
                        "json_raw": None,
                    }
                )
            logger.warning(
                f"Marked {len(sub_batch_pairs)} EANs as failed | "
                "Progress: "
                f"{eans_processed_so_far + eans_in_this_batch}/{total_eans} EANs"
            )

    return results
