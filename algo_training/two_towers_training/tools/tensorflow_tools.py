import pandas as pd
import tensorflow as tf
import mlflow

from tools.utils import connect_remote_mlflow


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


class MLFlowLogging(tf.keras.callbacks.Callback):
    def __init__(self, client_id: str, env: str, export_path: str):
        super(MLFlowLogging, self).__init__()
        self.client_id = client_id
        self.env = env
        self.export_path = export_path

    def on_epoch_end(self, epoch, logs=None):
        connect_remote_mlflow(self.client_id, env=self.env)
        mlflow.log_metrics(
            {
                "_train_loss": logs["loss"],
                "_val_loss": logs["val_loss"],
                "_val_top_5_categorical_accuracy": logs[
                    "val_factorized_top_k/top_5_categorical_accuracy"
                ],
                "_val_top_10_categorical_accuracy": logs[
                    "val_factorized_top_k/top_10_categorical_accuracy"
                ],
                "_val_top_50_categorical_accuracy": logs[
                    "val_factorized_top_k/top_50_categorical_accuracy"
                ],
                "_val_top_100_categorical_accuracy": logs[
                    "val_factorized_top_k/top_100_categorical_accuracy"
                ],
            },
            step=epoch,
        )
