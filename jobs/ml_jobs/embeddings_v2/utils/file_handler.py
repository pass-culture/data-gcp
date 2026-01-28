import concurrent.futures
import os

import numpy as np
from PIL import Image

from tools.config import IMAGE_DIR, MAX_PROCESS
from utils import get_filename_from_url
from utils.logging import logging


def _load_images(batch_urls):
    """
    Load an image from a file path.
    """

    images_bytes = []
    for url in batch_urls:
        filename = get_filename_from_url(url)
        # TODO: refactor this to remove the try if possible, otherwise an Exception more specialized
        try:
            img_path = os.path.join(IMAGE_DIR, f"{filename}.jpeg")
            with Image.open(img_path) as img:
                images_bytes.append(
                    {
                        "image": img.copy(),
                        "url": url,
                        "filename": filename,
                        "status": "success",
                    }
                )
        except Exception:
            images_bytes.append(
                {
                    "image": None,
                    "url": url,
                    "filename": filename,
                    "status": "failed",
                }
            )

    return images_bytes


def load_img_multiprocess(urls):
    """
    Download images using multiple processes.
    """

    batch_urls = np.array_split(urls, MAX_PROCESS)

    logging.info(f"Starting loading images with {MAX_PROCESS} CPUs")

    with concurrent.futures.ProcessPoolExecutor(MAX_PROCESS) as executor:
        images_stats = list(executor.map(_load_images, batch_urls))
    images_stats = [item for sublist in images_stats for item in sublist]
    return images_stats
