import time

import pandas as pd
import torch
import torch.nn.functional as F
import typer
from constants import HF_TOKEN_SECRET_NAME
from gcp_utils import get_secret
from loguru import logger
from transformers import AutoImageProcessor, AutoModel
from transformers.image_utils import load_image

MODEL_NAME = "facebook/dinov3-vitb16-pretrain-lvd1689m"
DF_BATCH_SIZE = 500
ENCODING_BATCH_SIZE = 16

app = typer.Typer()


def get_processor_and_model() -> tuple[AutoImageProcessor, AutoModel]:
    hf_token = get_secret(HF_TOKEN_SECRET_NAME)
    processor = AutoImageProcessor.from_pretrained(MODEL_NAME, token=hf_token)
    model = AutoModel.from_pretrained(MODEL_NAME, token=hf_token, device_map="auto")
    return processor, model


def get_cls_embeddings(
    processor: AutoImageProcessor, model: AutoModel, images: list, batch_size: int
) -> torch.Tensor:
    all_embeddings = []

    # Process images in batches to avoid OOM
    for i in range(0, len(images), batch_size):
        batch = images[i : i + batch_size]
        inputs = processor(images=batch, return_tensors="pt").to(model.device)
        with torch.inference_mode():
            outputs = model(**inputs)
        cls = outputs.last_hidden_state[:, 0, :]  # [batch_size, 768]
        all_embeddings.append(cls.cpu())

    # Concatenate all batches and L2-normalize
    embeddings = torch.cat(all_embeddings, dim=0)  # [len(images), 768]
    return F.normalize(embeddings, dim=1)  # L2-normalize


def custom_load_image(url: str):
    try:
        return load_image(url)
    except Exception as e:
        print(f"Error loading image from {url}: {e}")
        return None


def encode_images_on_batch_df(
    processor: AutoImageProcessor, model: AutoModel, batch_df: pd.DataFrame
):
    LOADED_IMAGE_COLUMN = "loaded_image"
    IMAGE_URL_COLUMN = "image_url"

    # Download images and filter out failed loads
    images_df = batch_df.assign(
        **{LOADED_IMAGE_COLUMN: batch_df[IMAGE_URL_COLUMN].map(custom_load_image)}
    ).dropna(subset=[LOADED_IMAGE_COLUMN])

    # Encode images and return results
    return images_df.assign(
        embedding=lambda df: list(
            get_cls_embeddings(
                processor=processor,
                model=model,
                images=df[LOADED_IMAGE_COLUMN].tolist(),
                batch_size=ENCODING_BATCH_SIZE,
            )
            .cpu()
            .numpy()
        )
    ).drop(
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

    # 3. Encode offer images
    results = []
    data_to_embed_df = offer_event_df.loc[
        lambda df: df["image_url"].notna(), ["offer_id", "image_url"]
    ]
    for batch_index, batch_df in data_to_embed_df.groupby(lambda x: x // DF_BATCH_SIZE):
        t0 = time.time()
        batch_embeddings_df = encode_images_on_batch_df(processor, model, batch_df)
        results.append(batch_embeddings_df)
        logger.info(
            f"Batch {batch_index + 1}/{(len(offer_event_df) // DF_BATCH_SIZE) + 1} "
            f"encoded in {time.time() - t0:.2f} seconds."
        )
    embeddings_df = pd.concat(results, ignore_index=True)

    # 4. Reconcile with original data and save results
    offer_event_df.merge(
        embeddings_df.loc[:, ["offer_id", "embedding"]],
        on="offer_id",
        how="left",
    ).to_parquet(offer_event_with_image_embeddings_filepath, index=False)


if __name__ == "__main__":
    app()
