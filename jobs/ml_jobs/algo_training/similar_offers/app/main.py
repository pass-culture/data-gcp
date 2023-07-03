from flask import Flask, request, Response, jsonify
from flask_cors import CORS
from loguru import logger
import tensorflow as tf

import numpy as np
import pandas as pd
import faiss

app = Flask(__name__)
CORS(app)


def load_model():
    tf_reco = tf.keras.models.load_model("./model/")
    offer_item_model = tf_reco.item_layer.layers[0].get_vocabulary()
    embedding_item_model = tf_reco.item_layer.layers[1].get_weights()
    model_weights = embedding_item_model[0]
    distance = len(model_weights[0])

    quantizer = faiss.IndexFlatL2(distance)
    index = faiss.IndexIVFPQ(quantizer, distance, 16, 8, 4)
    index.train(model_weights)
    index.add(model_weights)
    return index, model_weights, offer_item_model


class FaissModel:
    def __init__(self, faiss_index, model_weights, item_list):
        self.faiss_index = faiss_index
        self.item_dict = {}
        self.categories = {}
        self.item_list = item_list
        for idx, (x, y) in enumerate(zip(item_list, model_weights)):
            self.item_dict[x] = {"embeddings": y, "idx": idx}
        self.set_up_item_indexes()

    def set_up_item_indexes(self):
        df = pd.read_parquet("./metadata/item_metadata.parquet")
        for _, row in df.iterrows():
            idx = self.get_item_idx(row["item_id"])
            if idx is not None:
                self.categories.setdefault(row["category"], []).append(idx)

    def get_idx_from_categories(self, categories):
        result = []
        for cat in categories:
            result.extend(self.categories.get(cat, []))
        return result

    def get_offer_emb(self, item_id):
        embs = self.item_dict.get(item_id, None)
        if embs is not None:
            return np.array([embs["embeddings"]])
        else:
            return None

    def get_item_idx(self, item_id):
        embs = self.item_dict.get(item_id, None)
        if embs is not None:
            return embs["idx"]
        else:
            return None

    def selected_to_idx(self, selected_items):
        arr = []
        for item_id in selected_items:
            idx = self.get_item_idx(item_id)
            if idx is not None:
                arr.append(idx)
        return np.array(arr)

    def distances_to_offer(self, distances, indexes, n=10):
        if len(indexes) > n:
            distances = distances[:n]
            indexes = indexes[:n]
        out_preds, out_dist = [], []
        for (distance, index) in zip(distances, indexes):
            if index > 0:
                out_preds.append(self.item_list[index])
                out_dist.append(distance)

        return {"predictions": out_preds}

    def compute_distance(self, item_id, selected_idx=None, n=10):
        offer_emb = self.get_offer_emb(item_id)
        params = None
        if selected_idx is not None:
            params = faiss.SearchParametersIVF(sel=faiss.IDSelectorBatch(selected_idx))

        if offer_emb is not None:
            results = faiss_index.search(
                offer_emb,
                k=n,
                params=params,
            )
            return self.distances_to_offer(
                distances=results[0][0], indexes=results[1][0], n=n
            )
        return {"predictions": []}


logger.info("Load app...")

faiss_index, model_weights, item_list = load_model()
faiss_model = FaissModel(faiss_index, model_weights, item_list)

logger.info("Loaded model")


@app.route("/isalive")
def is_alive():
    status_code = Response(status=200)
    return status_code


@app.route("/predict", methods=["POST"])
def predict():
    logger.info("/predict!")

    req_json = request.get_json()
    input_json = req_json["instances"]
    item_id = input_json[0]["offer_id"]
    selected_categories = input_json[0].get("selected_categories", [])
    selected_idx = faiss_model.get_idx_from_categories(selected_categories)
    selected_offers = input_json[0].get("selected_offers", [])
    if len(selected_idx) == 0:
        selected_idx = None
    if len(selected_offers) > 0:
        selected_idx = faiss_model.selected_to_idx(selected_offers)
    try:
        n = int(input_json[0].get("size", 10))
    except:
        n = 10
    try:
        sim_offers = faiss_model.compute_distance(
            item_id, selected_idx=selected_idx, n=n
        )
        return jsonify(sim_offers)
    except Exception as e:
        logger.info(e)
        logger.info("error")
        return jsonify({"predictions": []})


if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=8080)
