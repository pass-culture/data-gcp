import os
import time

import pandas as pd
import typer
from loguru import logger

from src.utils import wikidata_checkpoint as checkpoint
from src.utils.qlever import clear_qlever_cache, fetch_wikidata_qlever_csv
from src.utils.wikidata_extraction import (
    HYDRATION_BATCH_DELAY_SECONDS,
    extract_wikidata_id,
    fetch_discovery,
    hydrate_batch,
)
from src.utils.wikidata_merge import merge_data, postprocess_data
from src.wikidata_config import QUERY_CONFIGS, render_query

app = typer.Typer()


@app.command()
def extract(
    query_name: str = typer.Option(),
    output_file_path: str = typer.Option(),
) -> None:
    """Fetch one extraction target from Wikidata and save its raw rows.

    Run once per key of QUERY_CONFIGS so a target-specific QLever failure only
    retries/fails that target instead of every other already-fetched target.

    For two-pass targets (QueryConfig.hydration_batch_size), an Airflow-level
    retry of this same task resumes from a local checkpoint (see
    src/utils/wikidata_checkpoint.py) instead of redoing Pass 1 and every
    already-hydrated Pass 2 batch.
    """
    if query_name not in QUERY_CONFIGS:
        raise typer.BadParameter(
            f"Unknown query_name {query_name!r}. Expected one of {list(QUERY_CONFIGS)}."
        )

    start_time = time.time()

    # Clear cache on qlever to prevent any resource issues
    clear_qlever_cache()

    logger.info(f"Fetch the data in CSV format for {query_name}")

    config = QUERY_CONFIGS[query_name]
    dropped_ids: list[str] = []
    checkpoint_dir = checkpoint.checkpoint_dir_for(query_name)
    if config.hydration_batch_size:
        logger.info(f"[{query_name}] Pass 1: discovering candidate entities")
        discovery_df = checkpoint.load_discovery_checkpoint(checkpoint_dir)
        if discovery_df is not None:
            logger.info(f"[{query_name}] Pass 1: resuming from checkpoint")
        else:
            discovery_df = fetch_discovery(query_name).pipe(extract_wikidata_id)
            checkpoint.save_discovery_checkpoint(checkpoint_dir, discovery_df)
        logger.info(
            f"[{query_name}] Pass 1: found {len(discovery_df)} candidate entities"
        )

        wikidata_ids = discovery_df["wikidata_id"].tolist()
        batch_size = config.hydration_batch_size
        batches = [
            wikidata_ids[i : i + batch_size]
            for i in range(0, len(wikidata_ids), batch_size)
        ]

        processed_batches = checkpoint.load_processed_batches(checkpoint_dir)
        dropped_ids = checkpoint.load_dropped_ids(checkpoint_dir)
        if processed_batches:
            logger.info(
                f"[{query_name}] Pass 2: resuming — {len(processed_batches)}/"
                f"{len(batches)} batches already hydrated in a previous attempt"
            )
        logger.info(
            f"[{query_name}] Pass 2: hydrating {len(wikidata_ids)} entities in "
            f"{len(batches)} batches of up to {batch_size}"
        )

        hydration_dfs: list[pd.DataFrame] = []
        for i, batch in enumerate(batches):
            if i in processed_batches:
                batch_df = checkpoint.load_batch_checkpoint(checkpoint_dir, i)
                if batch_df is not None:
                    hydration_dfs.append(batch_df)
                continue
            batch_dfs = hydrate_batch(query_name, batch, dropped_ids)
            if batch_dfs:
                batch_df = pd.concat(batch_dfs, ignore_index=True).pipe(
                    extract_wikidata_id
                )
                checkpoint.save_batch_checkpoint(checkpoint_dir, i, batch_df)
                hydration_dfs.append(batch_df)
            # Persist after every batch (not just at the end): dropped_ids and the
            # processed-batches log must reflect exactly what's been checkpointed
            # to disk so far, in case this attempt itself gets interrupted.
            checkpoint.save_dropped_ids(checkpoint_dir, dropped_ids)
            checkpoint.mark_batch_processed(checkpoint_dir, i)
            if i < len(batches) - 1:
                time.sleep(HYDRATION_BATCH_DELAY_SECONDS)

        # Inner merge: entities in dropped_ids simply have no row in hydration_df,
        # so they're naturally excluded here without extra filtering logic.
        df = (
            discovery_df.merge(
                pd.concat(hydration_dfs, ignore_index=True),
                on="wikidata_id",
                how="inner",
            )
            if hydration_dfs
            else pd.DataFrame()
        )
    else:
        query_string = render_query(query_name)
        logger.debug(f"SPARQL Query: \n{query_string}")
        df = fetch_wikidata_qlever_csv(query_string).pipe(extract_wikidata_id)

    if df.empty:
        if config.optional:
            logger.warning(f"No data retrieved for {query_name} — skipping raw file.")
            return
        error_message = f"No data retrieved for {query_name}."
        logger.error(error_message)
        raise ValueError(error_message)

    logger.info(f"Retrieved {len(df)} rows.")
    logger.info(f"Saving raw results to {output_file_path}")
    df.to_parquet(output_file_path, index=False)
    logger.info(f"Raw results saved successfully to {output_file_path}")

    if dropped_ids:
        logger.warning(
            f"{query_name}: dropped {len(dropped_ids)} entit"
            f"{'y' if len(dropped_ids) == 1 else 'ies'} QLever rejected as too "
            f"expensive even alone: {', '.join(dropped_ids)}"
        )

    elapsed = time.time() - start_time
    dropped_entity_word = "entity" if len(dropped_ids) == 1 else "entities"
    logger.info(
        f"[{query_name}] summary: {len(df)} rows, {len(dropped_ids)} "
        f"{dropped_entity_word} dropped, {elapsed:.1f}s elapsed, "
        f"saved to {output_file_path}"
    )

    # Only reached on success: a failed/raised attempt above leaves the checkpoint
    # in place on purpose, for the next Airflow-level retry to resume from.
    if config.hydration_batch_size:
        checkpoint.clear_checkpoint(checkpoint_dir)
        logger.info(f"[{query_name}] cleared hydration checkpoint at {checkpoint_dir}")


@app.command()
def merge(
    input_dir_path: str = typer.Option(
        help="Directory holding one <query_name>.parquet raw file per `extract` target."
    ),
    output_file_path: str = typer.Option(),
) -> None:
    """Merge and postprocess the raw per-target files produced by `extract`."""
    dfs: dict[str, pd.DataFrame] = {}

    for query_name, config in QUERY_CONFIGS.items():
        raw_file_path = os.path.join(input_dir_path, f"{query_name}.parquet")
        try:
            dfs[query_name] = pd.read_parquet(raw_file_path)
        except FileNotFoundError:
            if config.optional:
                logger.warning(f"{raw_file_path} not found — skipping {query_name}.")
                continue
            error_message = (
                f"Missing raw extraction for {query_name} at {raw_file_path}."
            )
            logger.error(error_message)
            raise ValueError(error_message) from None

    logger.info("Merging the data")
    merged_df = merge_data(dfs)

    logger.info("Postprocessing the data")
    postprocessed_df = postprocess_data(merged_df)
    logger.info(
        f"Found {len(postprocessed_df)} unique (wikidata_id, alias) pairs for {postprocessed_df.wikidata_id.nunique()} wikidata_ids"
    )

    logger.info(f"Saving results to {output_file_path}")
    postprocessed_df.to_parquet(output_file_path, index=False)
    logger.info(f"Results saved successfully to {output_file_path}")


if __name__ == "__main__":
    app()
