from flask import Flask, request, Response, jsonify
from flask_cors import CORS
from annoy import AnnoyIndex
from loguru import logger
import tensorflow as tf

app = Flask(__name__)
CORS(app)


def get_ann_model():
    # Load model
    ann_index = AnnoyIndex(64, "euclidean")
    ann_path = "./model/sim_offers/sim_offers.ann"
    ann_index.load(ann_path)

    # Load item_ids
    tf_reco = tf.keras.models.load_model("./model/tf_reco/")
    item_list_model = tf_reco.item_layer.layers[0].get_vocabulary()
    tf.keras.backend.clear_session()

    # Define AnnModel
    return AnnModel(item_list_model, ann_index)


class AnnModel:
    def __init__(self, item_ids, ann: AnnoyIndex):
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
            logger.info(f" sort: {len(selected_offers)}")
            for y in selected_offers:
                y_idx = self._get_offer_ann_idx(y)
                if y_idx:
                    nn_idx.append((y_idx, self.ann.get_distance(offer_id_idx, y_idx)))

            nn_idx = sorted(nn_idx, key=lambda tup: tup[1])
            if len(nn_idx) > n:
                nn_idx = nn_idx[:n]
            logger.info(f" done for: {len(selected_offers)}, out size {len(nn_idx)}")
            return [self.item_ids[index] for (index, _) in nn_idx]
        else:
            return []

    def _get_nn_offer_ids(self, x, n):
        nn_idx = self.ann.get_nns_by_item(x, n)
        nn_offer_ids = [self.item_ids[index] for index in nn_idx]
        return nn_offer_ids


Ann = get_ann_model()
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
        if selected_offers is None:
            logger.info(f"similar:{offer_id}")

            sim_offers = Ann.similar(input_offer_id=offer_id, n=n)
        else:
            logger.info(f"sort:{offer_id} on {len(selected_offers)}")
            sim_offers = Ann.sort(
                input_offer_id=offer_id, selected_offers=selected_offers, n=n
            )
        logger.info(f"out {len(sim_offers)}")
        return jsonify({"predictions": sim_offers})
    except Exception as e:
        logger.info(e)
        logger.info("error")
        return jsonify({"predictions": []})


if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=8080)
