from flask import Flask, request, Response, jsonify
from flask_cors import CORS

import tensorflow as tf
from tensorflow.keras.models import Model
from annoy import AnnoyIndex
import logging

app = Flask(__name__)
CORS(app)


class AnnModel(Model):
    def __init__(self, item_ids, ann):
        super().__init__(name="AnnModel")
        self.item_ids = item_ids
        offer_dict = {}
        for index, offer_id in enumerate(item_ids):
            offer_dict[offer_id] = index
        self.item_lookup = offer_dict
        self.ann = ann

    def _get_offer_ann_idx(self, input_offer_id):
        if input_offer_id in self.item_lookup:
            return self.item_lookup[input_offer_id]
        else:
            return None

    def similar(self, input_offer_id, n=10):
        """
        Returns list of most similar items from indexes
        """
        offer_id_idx = self._get_offer_ann_idx(input_offer_id)
        if offer_id_idx:
            return self._get_nn_offer_ids(offer_id_idx, n)
        else:
            return []

    def sort(self, input_offer_id, selected_offers, n=10):
        """
        Sort the list of given items
        """
        offer_id_idx = self._get_offer_ann_idx(input_offer_id)
        if offer_id_idx:
            nn_idx = []
            for y in selected_offers:
                y_idx = self._get_offer_ann_idx(y)
                if y_idx:
                    nn_idx.append((y_idx, self.ann.get_distance(offer_id_idx, y_idx)))
            nn_idx = sorted(nn_idx, key=lambda tup: tup[1])[:n]
            return [self.item_ids[index] for (index, _) in nn_idx]
        else:
            return []

    def _get_nn_offer_ids(self, x, n):
        nn_idx = self.ann.get_nns_by_item(x, n)
        nn_offer_ids = [self.item_ids[index] for index in nn_idx]
        return nn_offer_ids


# Load model
AnnIndex = AnnoyIndex(64, "euclidean")
ann_path = "./model/sim_offers/sim_offers.ann"
AnnIndex.load(ann_path)

# Load item_ids
tf_reco = tf.keras.models.load_model("./model/tf_reco/")
item_list_model = tf_reco.item_layer.layers[0].get_vocabulary()

# Define AnnModel
Ann = AnnModel(item_list_model, AnnIndex)
logging.info("Loaded model")


@app.route("/isalive")
def is_alive():
    status_code = Response(status=200)
    return status_code


@app.route("/predict", methods=["POST"])
def predict():

    req_json = request.get_json()
    input_json = req_json["instances"]
    offer_id = input_json[0]["offer_id"]
    selected_offers = input_json[0].get("selected_offers", None)
    n = input_json[0].get("size", 10)
    if selected_offers is None:
        logging.info(f"similar:{offer_id}")
        sim_offers = Ann.similar(input_offer_id=offer_id, n=n)
    else:
        logging.info(f"sort:{offer_id} on {len(selected_offers)}")
        sim_offers = Ann.sort(
            input_offer_id=offer_id, selected_offers=selected_offers, n=n
        )
    return jsonify({"predictions": sim_offers})


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8080)
