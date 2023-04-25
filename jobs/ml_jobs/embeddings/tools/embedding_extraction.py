import concurrent
import shutil
import socket
import time
import urllib.request
from itertools import repeat
from multiprocessing import cpu_count

from PIL import Image
from sentence_transformers import SentenceTransformer
from tools.logging_tools import log_duration

socket.setdefaulttimeout(30)


def extract_embedding(
    df_data,
    params,
):
    """
    Extarct embedding with pretrained models
    Two types available:
    - image :
        - Input: list of urls
    - text  :
        - Input: list of string
    """
    start = time.time()
    df_analysis = df_data.copy()
    for feature in params["features"]:
        feature_name = feature["name"]
        if feature["type"] == "image":
            model = SentenceTransformer("clip-ViT-B-32")
            urls = df_analysis.image_url.tolist()
            df_analysis[f"{feature_name}_embedding"] = encode_img_from_urls(model, urls)
            df_analysis[f"{feature_name}_embedding"] = df_analysis[
                f"{feature_name}_embedding"
            ].astype(str)
        if feature["type"] == "text":
            model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
            embeddings = model.encode(df_analysis[feature_name].tolist())
            df_analysis[f"{feature_name}_embedding"] = [
                list(embedding) for embedding in embeddings
            ]
            df_analysis[f"{feature_name}_embedding"] = df_analysis[
                f"{feature_name}_embedding"
            ].astype(str)
    log_duration(f"Embedding extraction: ", start)
    return df_analysis


def encode_img_from_urls(model, urls):
    index = 0
    offer_img_embs = []
    offer_wo_img = 0
    download_img_multiprocess(urls)
    for index in range(len(urls)):
        try:
            img_emb = model.encode(Image.open(f"./img/{index}.jpeg"))
            offer_img_embs.append(list(img_emb))
        except:
            offer_img_embs.append([0] * 512)
            offer_wo_img += 1
    print(f"{(offer_wo_img*100)/len(urls)}% offers dont have image")
    print("Removing image on local disk...")
    shutil.rmtree("./img")
    return offer_img_embs


def download_img_multiprocess(urls):
    max_process = cpu_count() - 1
    subset_length = len(urls) // max_process
    subset_length = subset_length if subset_length > 0 else 1
    batch_number = max_process if subset_length > 1 else 1
    print(
        f"Starting process... with {batch_number} CPUs, subset length: {subset_length} "
    )
    with concurrent.futures.ProcessPoolExecutor(batch_number) as executor:
        futures = executor.map(
            _download_img_from_url_list,
            repeat(urls),
            repeat(subset_length),
            repeat(batch_number),
            range(batch_number),
        )
    print("Multiprocessing done")
    return


def _download_img_from_url_list(urls, subset_length, batch_number, batch_id):
    try:
        temp_urls = urls[batch_id * subset_length : (batch_id + 1) * subset_length]
        index = batch_id if batch_id == 0 else (batch_id * subset_length)
        if batch_id == (batch_number - 1):
            temp_urls = urls[batch_id * subset_length :]
        for url in temp_urls:
            STORAGE_PATH_IMG = f"./img/{index}"
            __download_img_from_url(url, STORAGE_PATH_IMG)
            index += 1
        return
    except:
        return


def __download_img_from_url(url, storage_path):
    try:
        urllib.request.urlretrieve(url, f"{storage_path}.jpeg")
        return
    except:
        return None
