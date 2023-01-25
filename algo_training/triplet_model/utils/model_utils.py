import numpy as np
import tensorflow as tf


def identity_loss(y_true, y_pred):
    """Ignore y_true and return the mean of y_pred

    This is a hack to work-around the design of the Keras API that is
    not really suited to train networks with a triplet loss by default.
    """
    return tf.reduce_mean(y_pred)


def predict(match_model):
    user_id = "19373"
    items_to_rank = np.array(
        ["offer-7514002", "product-2987109", "offer-6406524", "toto", "tata"]
    )
    repeated_user_id = np.empty_like(items_to_rank)
    repeated_user_id.fill(user_id)
    predicted = match_model.predict([repeated_user_id, items_to_rank], batch_size=4096)
    return predicted
