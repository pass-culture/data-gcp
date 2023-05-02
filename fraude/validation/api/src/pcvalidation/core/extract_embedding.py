import os
import shutil
import urllib.request
import uuid

import numpy as np
from pcvalidation.utils.env_vars import IMAGE_MODEL, TEXT_MODEL
from PIL import Image


def extract_embedding(
    data,
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
    data_analysis = data
    for feature in params:
        feature_name = feature["name"]
        print(f"Embedding extraction for {feature_name} on going...")
        if feature["type"] == "image":
            model = IMAGE_MODEL
            url = data_analysis[feature_name]
            data_analysis[f"{feature_name}_embedding"] = encode_img_from_url(
                IMAGE_MODEL, url
            )
            try:
                del data_analysis[feature_name]
            except KeyError:
                pass
        if feature["type"] == "text":
            model = TEXT_MODEL
            embedding = model.encode(data_analysis[feature_name])
            data_analysis[f"{feature_name}_embedding"] = embedding
            # TODO: Once offer_name and description out of training remove HERE
            # try:
            #     del data_analysis[feature_name]
            # except KeyError:
            #     pass
    return data_analysis


def encode_img_from_url(model, url):
    index = 0
    offer_img_embs = []
    offer_wo_img = 0
    unique_id = print(str(uuid.uuid4()))
    os.makedirs(f"./img_{unique_id}", exist_ok=True)
    _download_img_from_url(url, f"./img_{unique_id}/{index}")
    try:
        img_emb = model.encode(Image.open(f"./img_{unique_id}/{index}.jpeg"))
        offer_img_embs = img_emb
    except:
        offer_img_embs = np.array([0] * 512)
        offer_wo_img += 1
    # print(f"{(offer_wo_img*100)/len(url)}% offers dont have image")
    # print("Removing image on local disk...")
    shutil.rmtree(f"./img_{unique_id}")
    return offer_img_embs


def _download_img_from_url(url, storage_path):
    try:
        urllib.request.urlretrieve(url, f"{storage_path}.jpeg")
        return
    except:
        return None
