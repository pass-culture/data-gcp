from annoy import AnnoyIndex
import tensorflow as tf
from utils import TRAIN_DIR, ENV_SHORT_NAME
from pathlib import Path


def save(model):
    dir = f"{TRAIN_DIR}/{ENV_SHORT_NAME}/sim_offers/"
    Path("dir").mkdir(parents=True, exist_ok=True)
    print("ann_path: ", dir)
    model.save(f"{dir}/sim_offers.ann")


def train():
    model = tf.keras.models.load_model(f"{TRAIN_DIR}/{ENV_SHORT_NAME}/tf_reco/")
    embedding_item_model = model.item_layer.layers[1].get_weights()

    annoy_model = AnnoyIndex(64, "euclidean")
    for index, vector in enumerate(embedding_item_model[0].tolist()):
        annoy_model.add_item(index, vector)
    annoy_model.build(-1)
    save(annoy_model)


if __name__ == "__main__":
    train()
