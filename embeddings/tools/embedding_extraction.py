import json
import time
from sentence_transformers import SentenceTransformer
import urllib.request
from tools.logging_tools import log_duration
from PIL import Image
from tqdm import tqdm
import os


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
            urls = df_analysis.image_url
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
    for url in tqdm(urls):
        STORAGE_PATH_IMG = f"./img/{index}"
        _download_img_from_url(url, STORAGE_PATH_IMG)
        try:
            img_emb = model.encode(Image.open(f"{STORAGE_PATH_IMG}.jpeg"))
            offer_img_embs.append(list(img_emb))
            os.remove(f"{STORAGE_PATH_IMG}.jpeg")
            index += 1
        except:
            offer_img_embs.append([0] * 512)
            index += 1
            offer_wo_img += 1
    print(f"{(offer_wo_img*100)/len(urls)}% offers dont have image")
    return offer_img_embs


def _download_img_from_url(url, storage_path):
    try:
        urllib.request.urlretrieve(url, f"{storage_path}.jpeg")
    except:
        return None
