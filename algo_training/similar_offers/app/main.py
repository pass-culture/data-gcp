from flask import Flask, request, Response, jsonify
from flask_cors import CORS

import tensorflow as tf
from tensorflow.keras.models import Model
from annoy import AnnoyIndex
from utils import SIMILAR_OFFERS_DIR

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

    def predict(self, input_offer_id):
        offer_id_idx = self.item_lookup[input_offer_id]
        nn_offer_ids = self._get_nn_offer_ids(offer_id_idx)
        return nn_offer_ids

    def _get_nn_offer_ids(self, x):
        nn_idx = self.ann.get_nns_by_item(x, 10)
        nn_offer_ids = [self.item_ids[index] for index in nn_idx]
        return nn_offer_ids


# Load model
AnnIndex = AnnoyIndex(64, "euclidean")
ann_path = "./model/sim_offers.ann"
AnnIndex.load(ann_path)

# Load item_ids
tf_reco = tf.keras.models.load_model("./model/tf_reco/model")
item_list_model = tf_reco.item_layer.layers[0].get_vocabulary()

# Define AnnModel
Ann = AnnModel(item_list_model, AnnIndex)
print("Loaded model")


@app.route("/isalive")
def is_alive():
    print("/isalive request")
    status_code = Response(status=200)
    return status_code


@app.route("/predict", methods=["POST"])
def predict():
    req_json = request.get_json()
    input_json = req_json["instances"]
    offer_id = input_json[0]["offer_id"]
    sim_offers = Ann.predict(input_offer_id=offer_id)
    return jsonify({"predictions": sim_offers})


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8080)
