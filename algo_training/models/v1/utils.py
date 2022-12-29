import random

import numpy as np
import tensorflow as tf


def identity_loss(y_true, y_pred):
    """Ignore y_true and return the mean of y_pred

    This is a hack to work-around the design of the Keras API that is
    not really suited to train networks with a triplet loss by default.
    """
    return tf.reduce_mean(y_pred)


def sample_triplets(positive_data, item_ids):
    """Sample negatives at random"""

    user_ids = positive_data["user_id"].values
    positive_item_ids = positive_data["item_id"].values
    negative__item_ids = np.array(
        random.choices(item_ids, k=len(user_ids)), dtype=object
    )

    return [user_ids, positive_item_ids, negative__item_ids]


def predict(match_model):
    user_id = "19373"
    items_to_rank = np.array(
        ["offer-7514002", "product-2987109", "offer-6406524", "toto", "tata"]
    )
    user_to_rank = np.array([user_id])
    user_and_offers_to_rank = np.array([user_id])
    user_and_offers_to_rank = np.concatenate((user_to_rank, items_to_rank))
    predicted = match_model.call(user_and_offers_to_rank)
    return predicted
