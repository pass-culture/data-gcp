"""Shared utilities for processing EANs via Titelive API."""

import requests
from connectors.titelive.client import TiteliveConnector
from workflows.titelive.config import (
    DEFAULT_BATCH_SIZE,
    MUSIC_SUBCATEGORIES,
    TiteliveCategory,
)
from workflows.titelive.logging_utils import get_logger
from workflows.titelive.transform import transform_api_response

logger = get_logger(__name__)


def process_eans_batch(
    api_client: TiteliveConnector,
    ean_pairs: list[tuple[str, str]],
    sub_batch_size: int = DEFAULT_BATCH_SIZE,
    verbose: bool = False,
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
        verbose: If True, log detailed per-sub-batch progress (default False)

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

    results = []

    # Process music EANs
    if music_eans:
        results.extend(
            _process_eans_by_base(
                api_client, music_eans, TiteliveCategory.MUSIC, sub_batch_size, verbose
            )
        )

    # Process paper EANs
    if paper_eans:
        results.extend(
            _process_eans_by_base(
                api_client, paper_eans, TiteliveCategory.PAPER, sub_batch_size, verbose
            )
        )

    return results


def _process_eans_by_base(
    api_client: TiteliveConnector,
    ean_pairs: list[tuple[str, str]],
    base: str,
    sub_batch_size: int,
    verbose: bool = False,
) -> list[dict]:
    """
    Process EANs for a specific base category in sub-batches.

    Args:
        api_client: Titelive API client
        ean_pairs: List of (ean, subcategoryid) tuples
        base: API base category ('music' or 'paper')
        sub_batch_size: Number of EANs per API call
        verbose: If True, log per-sub-batch progress

    Returns:
        List of dicts with processing results
    """
    results = []
    total_eans = len(ean_pairs)
    total_sub_batches = (total_eans + sub_batch_size - 1) // sub_batch_size

    for i in range(0, len(ean_pairs), sub_batch_size):
        sub_batch_pairs = ean_pairs[i : i + sub_batch_size]
        sub_batch_eans = [ean for ean, _ in sub_batch_pairs]
        current_sub_batch = (i // sub_batch_size) + 1

        try:
            # Call API with base parameter
            api_response = api_client.get_by_eans_with_base(sub_batch_eans, base)

            # Transform response
            transformed_df = transform_api_response(api_response)

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

            if verbose:
                logger.info(
                    f"  Sub-batch {current_sub_batch}/{total_sub_batches} ({base}): "
                    f"{len(returned_eans)} processed, {len(missing_eans)} deleted"
                )

        except requests.exceptions.HTTPError as e:
            # Handle 404 errors by processing EANs individually
            if e.response.status_code == 404:
                if sub_batch_size == 1:
                    # Single EAN returned 404 - mark as deleted_in_titelive
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
                    # Batch returned 404 - process each individually
                    logger.warning(
                        f"404 error for sub-batch {current_sub_batch}/"
                        f"{total_sub_batches} ({base}). "
                        f"Processing {len(sub_batch_pairs)} EANs individually."
                    )
                    individual_results = _process_eans_by_base(
                        api_client,
                        sub_batch_pairs,
                        base,
                        sub_batch_size=1,
                        verbose=False,
                    )
                    results.extend(individual_results)
            else:
                # Mark all EANs as failed for other HTTP errors
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

        except Exception as e:
            # Mark all EANs as failed for non-HTTP exceptions
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

    return results
