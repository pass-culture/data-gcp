import pandas as pd
import tensorflow as tf
import mlflow

from tools.utils import connect_remote_mlflow


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
