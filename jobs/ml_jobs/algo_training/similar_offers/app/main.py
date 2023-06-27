from flask import Flask, request, Response, jsonify
from flask_cors import CORS
from loguru import logger
import tensorflow as tf

import numpy as np
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
    def __init__(self, faiss_index, model_weights, offer_list):
        self.faiss_index = faiss_index
        self.item_dict = {}
        self.offer_list = offer_list
        for idx, (x, y) in enumerate(zip(offer_list, model_weights)):
            self.item_dict[x] = {"embeddings": y, "idx": idx}

    def get_offer_emb(self, offer_id):
        embs = self.item_dict.get(offer_id, None)
        if embs is not None:
            return np.array([embs["embeddings"]])
        else:
            return None

    def get_offer_idx(self, offer_id):
        embs = self.item_dict.get(offer_id, None)
        if embs is not None:
            return embs["idx"]
        else:
            return None

    def selected_to_idx(self, selected_offers):
        arr = []
        for offer_id in selected_offers:
            idx = self.get_offer_idx(offer_id)
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
                out_preds.append(self.offer_list[index])
                out_dist.append(distance)

        return {"predictions": out_preds, "distance": out_dist}

    def compute_distance(self, offer_id, selected_offers=None, n=10):
        offer_emb = self.get_offer_emb(offer_id)
        params = None
        if selected_offers is not None:
            subset_offer_indexes = self.selected_to_idx(selected_offers)
            params = faiss.SearchParametersIVF(
                sel=faiss.IDSelectorArray(subset_offer_indexes)
            )

        if offer_emb is not None:
            results = faiss_index.search(
                offer_emb,
                k=n,
                params=params,
            )
            return self.distances_to_offer(
                distances=results[0][0], indexes=results[1][0], n=n
            )
        return {"predictions": [], "distance": []}


faiss_index, model_weights, offer_list = load_model()
faiss_model = FaissModel(faiss_index, model_weights, offer_list)

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
    offer_id = input_json[0]["offer_id"]
    selected_offers = input_json[0].get("selected_offers", None)
    try:
        n = int(input_json[0].get("size", 10))
    except:
        n = 10
    try:
        sim_offers = faiss_model.compute_distance(offer_id, selected_offers, n=n)
        logger.info(f"out {len(sim_offers)}")
        return jsonify(sim_offers)
    except Exception as e:
        logger.info(e)
        logger.info("error")
        return jsonify(
            {"predictions": [], "distance": [], "error": f"An error occurred: {str(e)}"}
        )


if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=8080)
