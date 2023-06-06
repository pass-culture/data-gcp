import requests
import numpy as np
from PIL import Image


def extract_embedding(data, params, prepoc_models):
    """
    Extract embedding with pretrained models
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
    for feature in params:
        if feature["type"] == "image":
            model = prepoc_models[feature["type"]]
            url = data[feature["name"]]
            data[f"""{feature["name"]}_embedding"""] = _encode_img_from_url(model, url)
            try:
                del data[feature["name"]]
            except KeyError:
                pass
        if feature["type"] == "text":
            model = prepoc_models[feature["type"]]
            embedding = model.encode(data[feature["name"]])
            data[f"""{feature["name"]}_embedding"""] = embedding
    return data


def _encode_img_from_url(model, url):
    """
    Encode image with pre-trained model from url

    inputs:
        - model : HugginFaces pre-trained model using Sentence-Transformers
        - url : string of image url
    """
    offer_img_embs = []
    try:
        img_emb = model.encode(Image.open(io.BytesIO(requests.get(url).content)))
        offer_img_embs = img_emb
    except:
        offer_img_embs = np.array([0] * 512)
    return offer_img_embs
