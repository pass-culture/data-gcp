import os
import shutil
import time

from sentence_transformers import SentenceTransformer

from tools.config import FINGERPRINT_COLUMN, PRIMARY_KEY, TRANSFORMER_BATCH_SIZE
from utils.download import IMAGE_DIR, download_img_multiprocess
from utils.file_handler import load_img_multiprocess
from utils.logging import log_duration, logging


def extract_embedding(df_data, params):
    """
    Extract embeddings with pretrained models.
    Handles both image and text inputs.
    """
    models = load_models(params["models"])
    start = time.time()

    cols_to_preserve = [PRIMARY_KEY]
    if FINGERPRINT_COLUMN in df_data.columns:
        cols_to_preserve.append(FINGERPRINT_COLUMN)
    df_encoded = df_data[cols_to_preserve].astype(str)

    # Download images in parallel
    os.makedirs(IMAGE_DIR, exist_ok=True)
    download_img_multiprocess(df_data.image.tolist())

    for feature in params["features"]:
        for model_type in feature["model"]:
            step_time = time.time()
            model = models[model_type]
            emb_col_name = create_embedding_column_name(feature, model_type)
            if feature["type"] == "image":
                df_encoded[emb_col_name] = encode_img_from_path(
                    model, df_data.image.tolist()
                )

            elif feature["type"] in ["text", "macro_text"]:
                df_encoded[emb_col_name] = encode_text(
                    model, df_data[feature["name"]].tolist()
                )
            df_encoded[emb_col_name] = df_encoded[emb_col_name].astype(str)
            log_duration(f"Processed {feature['name']}, using {model_type}", step_time)

    shutil.rmtree(IMAGE_DIR, ignore_errors=True)
    log_duration("Done processing.", start)
    return df_encoded


def load_models(model_params):
    """
    Load and return the models specified in model_params.
    """
    return {
        model_name: SentenceTransformer(model_path)
        for model_name, model_path in model_params.items()
    }


def create_embedding_column_name(feature, model_type):
    """
    Create a standardized embedding column name.
    """
    emb_name_suffix = "" if model_type != "hybrid" else "_hybrid"
    return f"{feature['name']}{emb_name_suffix}_embedding"


def encode_img_from_path(model, paths):
    """
    Encode images from local paths using the specified model.
    """
    start = time.time()
    images_stats = load_img_multiprocess(urls=paths)
    log_duration("Load all images", start)
    start = time.time()

    images = [info["image"] for info in images_stats if info["status"] == "success"]
    urls = [info["url"] for info in images_stats if info["status"] == "success"]

    stats = {
        "total_images": len(paths),
        "encoded_images": 0,
        "missing_images": len(paths) - len(images),
        "failed_encodings": 0,
    }

    embeddings = {}
    if len(images) > 0:
        try:
            encoded_images = model.encode(
                images,
                batch_size=TRANSFORMER_BATCH_SIZE,
                normalize_embeddings=True,
                show_progress_bar=False,
            )
            for url, img_emb in zip(urls, encoded_images):
                embeddings[url] = list(img_emb)
            stats["encoded_images"] = len(encoded_images)
        except Exception as e:
            logging.error(f"Error encoding images: {e}")
            stats["failed_encodings"] = len(images)

        logging.info(
            f"{(stats['missing_images'])} over {stats['total_images']} of images are missing"
        )
        logging.info(
            f"{(stats['failed_encodings'])} over {stats['total_images']}  of images failed to encode"
        )

    log_duration("Predict all images", start)
    return [embeddings.get(url, [0] * 512) for url in paths]


def encode_text(model, texts):
    """
    Encode texts using the specified model.
    """
    return [
        list(embedding)
        for embedding in model.encode(
            texts,
            batch_size=TRANSFORMER_BATCH_SIZE,
            normalize_embeddings=True,
            show_progress_bar=False,
        )
    ]
