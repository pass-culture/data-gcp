import concurrent.futures
import os

import numpy as np
import requests

from tools.config import IMAGE_DIR, MAX_PROCESS
from utils import get_filename_from_url
from utils.logging import logging


def download_img_multiprocess(urls):
    """
    Download images using multiple processes.
    """
    batch_urls = np.array_split(urls, MAX_PROCESS)

    logging.info(f"Starting downloading images with {MAX_PROCESS} CPUs")

    with concurrent.futures.ProcessPoolExecutor(MAX_PROCESS) as executor:
        stats = list(executor.map(_download_img_from_url_list, batch_urls))
    # unnest
    stats = [item for sublist in stats for item in sublist]
    status_counts = {}
    for stat in stats:
        status = stat["status"]
        if status in status_counts:
            status_counts[status] += 1
        else:
            status_counts[status] = 1

    for status, count in status_counts.items():
        logging.info(f"Downloads - Stats -> Status '{status}' count: {count}")


def _download_img_from_url_list(urls):
    """
    Download a list of images.
    """

    stats = []
    for url in urls:
        try:
            response = requests.get(url, timeout=10)
            if (
                response.status_code == 200
                and int(response.headers.get("Content-Length", 0)) > 500
            ):
                filename = get_filename_from_url(url)
                img_path = os.path.join(IMAGE_DIR, f"{filename}.jpeg")
                with open(img_path, "wb") as f:
                    f.write(response.content)
                stats.append({"status": "success", "url": url})
            else:
                stats.append({"status": "skipped", "url": url})
        except requests.exceptions.RequestException:
            stats.append({"status": "error", "url": url})
        except Exception:
            stats.append({"status": "error", "url": url})
    return stats
