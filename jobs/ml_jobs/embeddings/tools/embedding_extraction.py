import concurrent
import numpy as np
import shutil
import time
from itertools import repeat
from multiprocessing import cpu_count

from PIL import Image
from sentence_transformers import SentenceTransformer
from tools.logging_tools import log_duration


def extract_embedding(
    df_data,
    params,
):
    """
    Extract embedding with pretrained models
    Two types available:
    - image :
        - Input: list of urls
    - text  :
        - Input: list of string
    """
    text_model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
    image_model = SentenceTransformer("clip-ViT-B-32")
    start = time.time()
    for feature in params["features"]:
        if feature["type"] == "image":
            urls = df_data.image_url.tolist()
            df_data[f"""{feature["name"]}_embedding"""] = encode_img_from_urls(
                image_model, urls
            )
        if feature["type"] == "text":
            df_data[f"""{feature["name"]}_embedding"""] = [
                list(embedding)
                for embedding in text_model.encode(df_data[feature["name"]].tolist())
            ]

        df_data[f"""{feature["name"]}_embedding"""] = df_data[
            f"""{feature["name"]}_embedding"""
        ].astype(str)
    log_duration(f"Embedding extraction: ", start)
    return df_data


def encode_img_from_urls(model, urls):
    offer_img_embs = []
    offer_wo_img = 0
    download_img_multiprocess(urls)
    for url in urls:
        url = str(url).replace("/", "-")
        try:
            img_emb = model.encode(Image.open(f"./img/{url}.jpeg"))
            offer_img_embs.append(list(img_emb))
        except:
            offer_img_embs.append([0] * 512)
            offer_wo_img += 1
    print(f"{(offer_wo_img*100)/len(urls)}% offers dont have image")
    print("Removing image on local disk...")
    shutil.rmtree("./img", ignore_errors=True)
    return offer_img_embs


def download_img_multiprocess(urls):
    max_process = cpu_count() - 2
    subset_length = len(urls) // max_process
    subset_length = subset_length if subset_length > 0 else 1
    batch_number = max_process if subset_length > 1 else 1
    print(
        f"Starting process... with {batch_number} CPUs, subset length: {subset_length} "
    )
    batch_urls = [list(chunk) for chunk in list(np.array_split(urls, batch_number))]
    with concurrent.futures.ProcessPoolExecutor(batch_number) as executor:
        futures = executor.map(
            _download_img_from_url_list,
            batch_urls,
        )
    print("Multiprocessing done")
    return


def _download_img_from_url_list(urls):
    import requests

    try:
        for url in urls:
            try:
                response = requests.get(url, timeout=10)
                if (
                    response.status_code == 200
                    and int(response.headers.get("Content-Length")) > 500
                ):
                    url = str(url).replace("/", "-")
                    filename = f"./img/{url}.jpeg"
                    with open(filename, "wb") as f:
                        f.write(response.content)
            except:
                continue
        return
    except:
        return
