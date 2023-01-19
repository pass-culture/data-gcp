import pandas as pd
import tensorflow as tf


def build_dict_dataset(
    data: pd.DataFrame, feature_names: list, batch_size: int, seed: int = None
):
    """
    Builds a tf dataset where each row is a dict containing all the features stated in `feature_names`
    """
    return (
        tf.data.Dataset.from_tensor_slices(data.values)
        .map(lambda x: {column: x[idx] for idx, column in enumerate(feature_names)})
        .batch(batch_size=batch_size, drop_remainder=False)
        .shuffle(buffer_size=10 * batch_size, seed=seed, reshuffle_each_iteration=False)
    )
