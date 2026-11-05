import os
import time

import pandas as pd
import typer
from loguru import logger

from src.extraction.wikidata_config import QUERY_CONFIGS
from src.extraction.wikidata_extraction import extract_query
from src.extraction.wikidata_merge import merge_data, postprocess_data

app = typer.Typer()


def _save_raw_results(df: pd.DataFrame, output_file_path: str) -> None:
    logger.info(f"Retrieved {len(df)} rows.")
    logger.info(f"Saving raw results to {output_file_path}")
    df.to_parquet(output_file_path, index=False)
    logger.info(f"Raw results saved successfully to {output_file_path}")


def _log_extraction_summary(
    query_name: str,
    df: pd.DataFrame,
    start_time: float,
    output_file_path: str,
) -> None:
    elapsed = time.time() - start_time
    logger.info(
        f"[{query_name}] summary: {len(df)} rows, {elapsed:.1f}s elapsed, "
        f"saved to {output_file_path}"
    )


@app.command()
def extract(
    query_name: str = typer.Option(),
    output_file_path: str = typer.Option(),
) -> None:
    """Fetch one extraction target from Wikidata and save its raw rows.

    Run once per key of QUERY_CONFIGS so a target-specific QLever failure only
    retries/fails that target instead of every other already-fetched target.
    """
    if query_name not in QUERY_CONFIGS:
        raise typer.BadParameter(
            f"Unknown query_name {query_name!r}. Expected one of {list(QUERY_CONFIGS)}."
        )

    start_time = time.time()
    logger.info(f"Fetch the data in CSV format for {query_name}")

    config = QUERY_CONFIGS[query_name]
    df = extract_query(query_name)

    if df.empty:
        if config.optional:
            logger.warning(f"No data retrieved for {query_name} — skipping raw file.")
            return
        error_message = f"No data retrieved for {query_name}."
        logger.error(error_message)
        raise ValueError(error_message)

    _save_raw_results(df, output_file_path)
    _log_extraction_summary(query_name, df, start_time, output_file_path)


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
