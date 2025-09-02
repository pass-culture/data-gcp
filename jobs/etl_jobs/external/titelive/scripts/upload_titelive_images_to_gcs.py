import json
import os
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import typer
from google.cloud import storage
from loguru import logger
from tqdm import tqdm

from src.utils.gcp import upload_image_to_gcs

app = typer.Typer()

INPUT_FILE_PATH_OPTION = typer.Option(..., help="Path to the input file")
GCS_THUMB_BASE_PATH_OPTION = typer.Option(..., help="GCS output directory")
OUTPUT_FILE_PATH_OPTION = typer.Option(..., help="Path to the output file")

MAX_WORKERS = (os.cpu_count() - 1) * 5


def _upload_recto_verso_images_to_gcs(
    row: pd.Series, storage_client: storage.Client, base_column_name: str
):
    """Upload image for a given row, handling missing URLs."""
    if pd.notna(row[f"{base_column_name}"]):
        return upload_image_to_gcs(
            storage_client=storage_client,
            base_image_url=row[f"{base_column_name}"],
            gcs_upload_url=row[f"{base_column_name}_gcs_path"],
        )
    else:
        return (False, None, f"No URL for {base_column_name}")


def _process_row_images(row, storage_client):
    """Process both recto and verso images for a single row."""
    recto_result = _upload_recto_verso_images_to_gcs(row, storage_client, "recto")
    verso_result = _upload_recto_verso_images_to_gcs(row, storage_client, "verso")
    return recto_result, verso_result


@app.command()
def upload_titelive_images_to_gcs(
    input_parquet_path: str = INPUT_FILE_PATH_OPTION,
    gcs_thumb_base_path: str = GCS_THUMB_BASE_PATH_OPTION,
    output_parquet_path: str = OUTPUT_FILE_PATH_OPTION,
):
    input_df = pd.read_parquet(input_parquet_path)

    # Extracting image urls and generating uuids
    images_url_df = pd.DataFrame.from_dict(
        input_df["article_imagesUrl"].apply(json.loads).tolist()
    ).assign(
        recto_uuid=lambda df: df.recto.where(
            df.recto.isna(), df.index.map(lambda s: uuid.uuid4())
        ),
        verso_uuid=lambda df: df.verso.where(
            df.verso.isna(), df.index.map(lambda s: uuid.uuid4())
        ),
        recto_gcs_path=lambda df: df.recto_uuid.where(
            df.recto_uuid.isna(),
            df.recto_uuid.map(lambda s: f"{gcs_thumb_base_path}/{s}"),
        ),
        verso_gcs_path=lambda df: df.verso_uuid.where(
            df.verso_uuid.isna(),
            df.verso_uuid.map(lambda s: f"{gcs_thumb_base_path}/{s}"),
        ),
    )

    # Uploading images to GCS
    storage_client = storage.Client()
    logger.info(f"Uploading  images to GCS using {MAX_WORKERS} workers")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(_process_row_images, row, storage_client): idx
            for idx, row in images_url_df.iterrows()
        }

        recto_results = {}
        verso_results = {}
        for future in tqdm(
            as_completed(futures), total=len(images_url_df), desc="Uploading images"
        ):
            idx = futures[future]
            recto_result, verso_result = future.result()
            recto_results[idx] = recto_result
            verso_results[idx] = verso_result

    final_df = images_url_df.assign(
        recto_upload_status=images_url_df.index.map(recto_results.get),
        verso_upload_status=images_url_df.index.map(verso_results.get),
    )

    # Merging and saving to GCS
    input_df.merge(
        final_df,
        left_index=True,
        right_index=True,
    ).astype(
        {
            "recto_upload_status": "string",
            "verso_upload_status": "string",
            "recto_uuid": "string",
            "verso_uuid": "string",
        }
    ).to_parquet(output_parquet_path)


if __name__ == "__main__":
    app()
