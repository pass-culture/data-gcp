import concurrent
import numpy as np
import shutil
import time
from multiprocessing import cpu_count

from PIL import Image
from sentence_transformers import SentenceTransformer
from tools.logging_tools import log_duration
from tools.config import ENV_SHORT_NAME


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
    MODEL_DICT = {}
    for model in params["models"]:
        MODEL_DICT[model] = SentenceTransformer(params["models"][model])
    start = time.time()
    emb_size_dict = {}
    df_encoded = df_data[["item_id"]].astype(str)
    download_img_multiprocess(df_data.image_url.tolist())
    for feature in params["features"]:
        for model_type in feature["model"]:
            model = MODEL_DICT[model_type]
            emb_name_suffix = "" if model_type != "hybrid" else "_hybrid"
            emb_col_name = f"""{feature["name"]}{emb_name_suffix}_embedding"""
            if feature["type"] == "image":
                df_encoded[emb_col_name] = encode_img_from_path(
                    model, df_data.image_url.tolist()
                )
                emb_size_dict[emb_col_name] = 512
            if feature["type"] in ["text", "macro_text"]:
                encode = model.encode(df_data[feature["name"]].tolist())
                df_encoded[emb_col_name] = [list(embedding) for embedding in encode]
                emb_size_dict[emb_col_name] = len(encode[0])
            df_encoded[emb_col_name] = df_encoded[emb_col_name].astype(str)
    print("Removing image on local disk...")
    shutil.rmtree("./img", ignore_errors=True)
    log_duration(f"Embedding extraction: ", start)
    return df_encoded, emb_size_dict


def encode_img_from_path(model, paths):
    offer_img_embs = []
    offer_wo_img = 0
    for url in paths:
        url = str(url).replace("/", "-")
        try:
            img_emb = model.encode(Image.open(f"./img/{url}.jpeg"))
            offer_img_embs.append(list(img_emb))
        except:
            offer_img_embs.append([0] * 512)
            offer_wo_img += 1
    print(f"{(offer_wo_img*100)/len(paths)}% offers dont have image")
    return offer_img_embs


def download_img_multiprocess(urls):
    max_process = 2 if ENV_SHORT_NAME == "dev" else cpu_count() - 2
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
