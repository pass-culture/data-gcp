import time

import pandas as pd
import torch
import torch.nn.functional as F
import tqdm
import typer
from loguru import logger
from transformers import AutoImageProcessor, AutoModel
from transformers.image_utils import load_image

from src.constants import (
    HF_TOKEN_SECRET_NAME,
    IMAGE_EMBEDDING_COLUMN,
    IMAGE_URL_COLUMN,
    OFFER_ID_COLUMN,
)
from src.gcp_utils import get_secret

MODEL_NAME = "facebook/dinov3-vitb16-pretrain-lvd1689m"
DF_BATCH_SIZE = 500
ENCODING_BATCH_SIZE = 16

app = typer.Typer()


def get_processor_and_model() -> tuple[AutoImageProcessor, AutoModel]:
    HF_TOKEN = get_secret(HF_TOKEN_SECRET_NAME)

    processor = AutoImageProcessor.from_pretrained(MODEL_NAME, token=HF_TOKEN)
    model = AutoModel.from_pretrained(MODEL_NAME, token=HF_TOKEN, device_map="auto")
    return processor, model


def get_cls_embeddings(
    processor: AutoImageProcessor, model: AutoModel, images: list, batch_size: int
) -> torch.Tensor:
    all_embeddings = []

    # Process images in batches to avoid OOM
    for i in range(0, len(images), batch_size):
        batch = images[i : i + batch_size]
        inputs = processor(images=batch, return_tensors="pt").to(
            model.device
        )  # Move inputs to the same device as the model
        with torch.inference_mode():
            outputs = model(**inputs)
        cls = outputs.last_hidden_state[:, 0, :]  # [batch_size, 768]
        all_embeddings.append(cls.cpu())

    # Concatenate all batches and L2-normalize
    embeddings = torch.cat(all_embeddings, dim=0)  # [len(images), 768]
    return F.normalize(embeddings, dim=1)  # L2-normalize


def custom_load_image(url: str) -> torch.Tensor | None:
    try:
        return load_image(url)
    except Exception as e:
        logger.error(f"Error loading image from {url}: {e}")
        return None


def encode_images_on_batch_df(
    processor: AutoImageProcessor, model: AutoModel, batch_df: pd.DataFrame
) -> pd.DataFrame:
    LOADED_IMAGE_COLUMN = "loaded_image"

    # Download images and filter out failed loads
    images_df = batch_df.assign(
        **{LOADED_IMAGE_COLUMN: batch_df[IMAGE_URL_COLUMN].map(custom_load_image)}
    ).dropna(subset=[LOADED_IMAGE_COLUMN])

    # Encode images and return results
    image_embeddings = (
        get_cls_embeddings(
            processor=processor,
            model=model,
            images=images_df[LOADED_IMAGE_COLUMN].tolist(),
            batch_size=ENCODING_BATCH_SIZE,
        )
        .cpu()
        .numpy()
    )
    return images_df.assign(**{IMAGE_EMBEDDING_COLUMN: list(image_embeddings)}).drop(
        columns=[LOADED_IMAGE_COLUMN]
    )  # We don't need to keep the loaded images in memory


@app.command()
def main(
    offer_event_filepath: str = typer.Option(),
    offer_event_with_image_embeddings_filepath: str = typer.Option(),
) -> None:
    # 1. Load raw data
    offer_event_df = pd.read_parquet(offer_event_filepath)

    # 2. Load model
    processor, model = get_processor_and_model()

    # 3. Prepare data to embed: keep only offers with image and relevant columns
    data_to_embed_df = offer_event_df.loc[
        lambda df: df[IMAGE_URL_COLUMN].notna(), [OFFER_ID_COLUMN, IMAGE_URL_COLUMN]
    ]

    # 4. Encode offer images
    results = []
    t0 = time.time()

    for _, batch_df in tqdm.tqdm(
        data_to_embed_df.groupby(lambda x: x // DF_BATCH_SIZE)
    ):
        batch_embeddings_df = encode_images_on_batch_df(processor, model, batch_df)
        results.append(batch_embeddings_df)
    embeddings_df = pd.concat(results, ignore_index=True)
    logger.success(
        f"Encoded {len(embeddings_df)} images in {time.time() - t0:.2f} seconds"
    )

    # 5. Reconcile with original data and save results
    offer_event_df.merge(
        embeddings_df.loc[:, [OFFER_ID_COLUMN, IMAGE_EMBEDDING_COLUMN]],
        on=OFFER_ID_COLUMN,
        how="left",
    ).to_parquet(offer_event_with_image_embeddings_filepath, index=False)


if __name__ == "__main__":
    app()
