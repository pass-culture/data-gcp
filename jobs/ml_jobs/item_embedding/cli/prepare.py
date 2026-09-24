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

import typer
from config import load_vector_config
from constants import ROWS_PER_CHUNK
from gcs_utils import iter_parquet_chunks, write_parquet
from loguru import logger
from preprocessing import apply_preprocessors
from prompt_building import build_prompts

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

    for i, chunk in enumerate(
        iter_parquet_chunks(
            input_parquets_folder_path, rows_per_chunk, required_columns=required
        )
    ):
        preprocessed = apply_preprocessors(chunk[required], vector.preprocessors)

        prompts = preprocessed[["item_id", "content_hash"]].copy()
        prompts["prompt"] = build_prompts(preprocessed, vector)

        # Drop items with no metadata to embed (empty prompt) so no empty prompt
        # ever reaches the encoder.
        kept = prompts[prompts["prompt"] != ""].reset_index(drop=True)
        dropped = len(prompts) - len(kept)
        if dropped:
            logger.warning(f"Chunk {i}: dropped {dropped} item(s) with empty prompt")

        output_path = f"{output_parquets_folder_path}/prompts_{i}.parquet"
        write_parquet(kept, output_path)
        logger.info(f"Wrote {len(kept)} prompts to {output_path}")

    logger.info("✅ Prompt preparation complete")


if __name__ == "__main__":
    app()
