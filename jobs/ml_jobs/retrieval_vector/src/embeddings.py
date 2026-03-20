import os

################################  To use Keras 2 instead of 3  ################################
# See [TensorFlow + Keras 2 backwards compatibility section](https://keras.io/getting_started/)
os.environ["TF_USE_LEGACY_KERAS"] = "1"
###############################################################################################

import numpy as np
import tensorflow as tf
from loguru import logger


def extract_embeddings_from_tt_model(model_path: str):
    # download model
    logger.info("Load Two Tower model...")
    tf_reco = tf.keras.models.load_model(model_path)
    logger.info("Two Tower model loaded.")

    # get user and item embeddings
    item_list = tf_reco.item_layer.layers[0].get_vocabulary()
    item_weights = tf_reco.item_layer.layers[1].get_weights()[0].astype(np.float32)
    user_list = tf_reco.user_layer.layers[0].get_vocabulary()
    user_weights = tf_reco.user_layer.layers[1].get_weights()[0].astype(np.float32)
    user_embedding_dict = {x: y for x, y in zip(user_list, user_weights)}
    item_embedding_dict = {x: y for x, y in zip(item_list, item_weights)}

    return user_embedding_dict, item_embedding_dict


def generate_dummy_embeddings(
    embedding_dimension: int,
    item_ids: list[str],
    user_ids: list[str],
):
    """Generate dummy embeddings for items and users."""
    user_embedding_dict = {
        user_id: np.random.random((embedding_dimension,)) for user_id in user_ids
    }
    item_embedding_dict = {
        item_id: np.random.random((embedding_dimension,)) for item_id in item_ids
    }
    return user_embedding_dict, item_embedding_dict
