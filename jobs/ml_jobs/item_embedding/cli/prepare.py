"""Step 1 of the embedding pipeline: prepare prompts (preprocess + build).

Reads a vector's input metadata from GCS, applies the preprocessors declared in
its YAML config, renders one prompt per item (via ``prompt_template`` or the
default ``"label : value"`` concatenation), drops items whose features are all
null (empty prompt), and writes ``item_id, content_hash, prompt`` back to GCS
for the embed step.

Run from the job root:
    uv run python -m cli.prepare \
        --config-file-name movies_metadata \
        --input-parquets-folder-path  gs://.../movies_metadata/input \
        --output-parquets-folder-path gs://.../movies_metadata/prompts
"""

from itertools import chain

import pandas as pd
import typer
from loguru import logger
from src.config import load_vector_config
from src.constants import ROWS_PER_CHUNK
from src.gcs_utils import iter_parquet_chunks, write_parquet
from src.preprocessing import apply_preprocessors
from src.prompt_building import build_prompts

app = typer.Typer(help="Preprocess a vector's metadata and build its prompts.")


@app.command()
def main(
    config_file_name: str = typer.Option(
        ..., help="Single-vector config name (without .yaml) in configs/"
    ),
    input_parquets_folder_path: str = typer.Option(
        ..., help="GCS folder with the vector's input metadata parquet files"
    ),
    output_parquets_folder_path: str = typer.Option(
        ..., help="GCS folder to write the prompts parquet files to"
    ),
    rows_per_chunk: int = typer.Option(ROWS_PER_CHUNK),
) -> None:
    vector = load_vector_config(config_file_name)
    required = ["item_id", "content_hash", *vector.features]
    logger.info(f"Preparing prompts for vector '{vector.name}'")

    chunk_iter = iter_parquet_chunks(
        input_parquets_folder_path, rows_per_chunk, required_columns=required
    )
    # Only "no input files" is tolerated: peek the first chunk inside the try so
    # a FileNotFoundError raised mid-stream is NOT swallowed (partial output).
    try:
        first_chunk = next(chunk_iter, None)
    except FileNotFoundError:
        # export_input's own `WHERE to_embed` matched zero rows this run, so
        # BigQuery's EXPORT DATA wrote no files at all under this vector's
        # input/ prefix -- nothing to prepare, not a real error.
        logger.info(f"No input files found at {input_parquets_folder_path}")
        first_chunk = None
    chunks = chain([first_chunk], chunk_iter) if first_chunk is not None else iter(())

    wrote_any = False
    for i, chunk in enumerate(chunks):
        preprocessed = apply_preprocessors(chunk[required], vector.preprocessors)

        prompts = preprocessed[["item_id", "content_hash"]].copy()
        prompts["prompt"] = build_prompts(preprocessed, vector)

        # Drop items with no metadata to embed (empty prompt) so no empty
        # prompt ever reaches the encoder.
        kept = prompts[prompts["prompt"] != ""].reset_index(drop=True)
        dropped = len(prompts) - len(kept)
        if dropped:
            logger.warning(f"Chunk {i}: dropped {dropped} item(s) with empty prompt")

        output_path = f"{output_parquets_folder_path}/prompts_{i}.parquet"
        write_parquet(kept, output_path)
        wrote_any = True
        logger.info(f"Wrote {len(kept)} prompts to {output_path}")

    if not wrote_any:
        # Nothing to embed this run (zero input rows, or zero input files as
        # above). Write an empty, schema-correct file anyway so the embed step
        # finds a (empty) dataset instead of a missing GCS prefix.
        empty = pd.DataFrame(columns=["item_id", "content_hash", "prompt"])
        output_path = f"{output_parquets_folder_path}/prompts_0.parquet"
        write_parquet(empty, output_path)
        logger.info(f"No items to prepare; wrote empty prompts file to {output_path}")

    logger.info("✅ Prompt preparation complete")


if __name__ == "__main__":
    app()
