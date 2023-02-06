import numpy as np
import tensorflow as tf
import pandas as pd


def load_triplets_dataset(
    input_data: pd.DataFrame,
    user_columns: list,
    item_columns: list,
    batch_size: int,
) -> tf.data.Dataset:
    """
    Params:
        - input_data: DataFrame containing user, item pairs with their corresponding features
        - user_columns: list of columns containing users' features
        - item_columns: list of columns containing items' features
        - batch_size: Size of dataset's batch
    Returns: A tf Dataset containing:
        - x = triplet containing (user_id, the item_id associated to the user, a random item_id)
        - y = fake data as we make no predictions
    """

    anchor_data = list(zip(*[input_data[col] for col in user_columns]))
    positive_data = list(zip(*[input_data[col] for col in item_columns]))
    input_data = input_data.sample(frac=1)
    negative_data = list(zip(*[input_data[col] for col in item_columns]))

    anchor_dataset = tf.data.Dataset.from_tensor_slices(anchor_data)
    positive_dataset = tf.data.Dataset.from_tensor_slices(positive_data)
    negative_dataset = (
        tf.data.Dataset.from_tensor_slices(negative_data)
        .shuffle(buffer_size=10 * batch_size)
        .repeat()
    )  # We shuffle the negative examples to get new random examples at each call

    x = tf.data.Dataset.zip((anchor_dataset, positive_dataset, negative_dataset))
    y = tf.data.Dataset.from_tensor_slices(np.ones((len(input_data), 3)))

    return tf.data.Dataset.zip((x, y)).batch(
        batch_size=batch_size, drop_remainder=False
    )
