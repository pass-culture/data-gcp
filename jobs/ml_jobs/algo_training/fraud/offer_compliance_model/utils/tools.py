import os
import shutil
import urllib.request
from heapq import nlargest, nsmallest

import numpy as np
import pandas as pd
from catboost import Pool
from PIL import Image
from sentence_transformers import SentenceTransformer
from tqdm import tqdm

import seaborn as sns

sns.set_theme()
sns.set(font_scale=1)

import os
import time
from multiprocessing import cpu_count
import concurrent
from itertools import repeat
import mlflow
from loguru import logger

STORAGE_PATH_IMG = "./img"


def prepare_features(df):
    columns = [col for col in df.columns.tolist() if col not in ("offer_validation")]
    for col in columns:
        if df[col].dtype == int or df[col].dtype == float:
            df[col] = df[col].fillna(0)
            df[col] = df[col].astype(int)
        elif df[col].dtype.name == "boolean":
            df[col] = np.where(df[col] == True, 1, 0)
        else:
            df[col] = df[col].fillna("")
            df[col] = df[col].astype(str)
    # Set target
    df["target"] = np.where(df["offer_validation"] == "APPROVED", 1, 0)
    # Remove useless columns
    df = df.drop(columns=["offer_validation"])
    return df


def extract_embedding(
    df_data,
    params,
    image_model,
    text_model,
):
    """
    Extarct embedding with pretrained models
    Two types available:
    - image :
        - Input: list of urls
    - text  :
        - Input: list of string
    """
    df_analysis = df_data.copy()
    for feature in params:
        start = time.time()
        feature_name = feature["name"]
        print(f"Embedding extraction for {feature_name} on going...")
        if feature["type"] == "image":
            model = image_model
            urls = df_analysis[feature_name].tolist()
            df_analysis[f"{feature_name}_embedding"] = encode_img_from_urls(model, urls)
            df_analysis = df_analysis.drop(columns=[feature_name])
        if feature["type"] == "text":
            model = text_model
            embeddings = model.encode(df_analysis[feature_name].tolist())
            df_analysis[f"{feature_name}_embedding"] = [
                list(embedding) for embedding in embeddings
            ]
        log_duration(f"Embedding extraction for : {feature_name} done in: ", start)
    return df_analysis


def encode_img_from_urls(model, urls):
    index = 0
    offer_img_embs = []
    offer_wo_img = 0
    os.makedirs("./img", exist_ok=True)
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

def get_individual_contribution(shap_values, df_data):
    topk_validation_factor = []
    topk_rejection_factor = []
    for i in range(len(df_data)):
        individual_shap_values = list(shap_values[i, :])
        klargest = nlargest(3, individual_shap_values)
        ksmallest = nsmallest(3, individual_shap_values)
        topk_validation_factor.append(
            [
                df_data.columns[individual_shap_values.index(max_val)]
                for max_val in klargest
            ]
        )
        topk_rejection_factor.append(
            [
                df_data.columns[individual_shap_values.index(min_val)]
                for min_val in ksmallest
            ]
        )
    return topk_validation_factor, topk_rejection_factor


def log_duration(message, start):
    logger.info(f"{message}: {time.time() - start} seconds.")
