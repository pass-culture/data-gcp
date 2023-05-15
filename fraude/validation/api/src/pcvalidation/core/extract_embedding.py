import os
import shutil
import urllib.request
import uuid

import numpy as np
from PIL import Image


def extract_embedding(data, params, text_model, image_model):
    """
    Extarct embedding with pretrained models
    Two types available:
    - image :
        - Input: list of urls
    - text  :
        - Input: list of string
    Params template:
    [
        {"name": "offer_name", "type": "text"},
        {"name": "offer_description", "type": "text"},
        {"name": "offer_image", "type": "image"},
    ]
    """
    data_analysis = data
    for feature in params:
        feature_name = feature["name"]
        print(f"Embedding extraction for {feature_name} on going...")
        if feature["type"] == "image":
            model = image_model
            url = data_analysis[feature_name]
            data_analysis[f"{feature_name}_embedding"] = _encode_img_from_url(
                model, url
            )
            try:
                del data_analysis[feature_name]
            except KeyError:
                pass
        if feature["type"] == "text":
            model = text_model
            embedding = model.encode(data_analysis[feature_name])
            data_analysis[f"{feature_name}_embedding"] = embedding
            # TODO: Once offer_name and description out of training remove HERE
            # try:
            #     del data_analysis[feature_name]
            # except KeyError:
            #     pass
    return data_analysis


def _encode_img_from_url(model, url):
    """
    Encode image with pre-trained model from url

    inputs:
        - model : HugginFaces pre-trained model using Sentence-Transformers
        - url : string of image url
    """
    index = 0
    offer_img_embs = []
    offer_wo_img = 0
    unique_id = print(str(uuid.uuid4()))
    os.makedirs(f"./img_{unique_id}", exist_ok=True)
    __download_img_from_url(url, f"./img_{unique_id}/{index}")
    try:
        img_emb = model.encode(Image.open(f"./img_{unique_id}/{index}.jpeg"))
        offer_img_embs = img_emb
    except:
        offer_img_embs = np.array([0] * 512)
        offer_wo_img += 1
    shutil.rmtree(f"./img_{unique_id}")
    return offer_img_embs


def __download_img_from_url(url, storage_path):
    try:
        urllib.request.urlretrieve(url, f"{storage_path}.jpeg")
        return
    except:
        return None
