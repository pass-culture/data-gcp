import mlflow
import tensorflow as tf

from utils.mlflow_tools import connect_remote_mlflow


class MLFlowLogging(tf.keras.callbacks.Callback):
    def __init__(self, export_path: str):
        super(MLFlowLogging, self).__init__()
        self.export_path = export_path

    def on_epoch_end(self, epoch, logs=None):
        connect_remote_mlflow()

        mlflow.log_metrics(
            {
                "loss": logs.get("loss", 0),
                "val_loss": logs.get("val_loss", 0),
                "val_top_010_categorical_accuracy": logs.get(
                    "val_factorized_top_k/top_10_categorical_accuracy", 0
                ),
                "val_top_050_categorical_accuracy": logs.get(
                    "val_factorized_top_k/top_50_categorical_accuracy", 0
                ),
            },
            step=epoch,
        )
