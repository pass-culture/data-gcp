import tensorflow as tf
import mlflow

from utils.mlflow_tools import connect_remote_mlflow
from utils.constants import ENV_SHORT_NAME
class MLFlowLogging(tf.keras.callbacks.Callback):
    def __init__(self, client_id: str, env: str, export_path: str):
        super(MLFlowLogging, self).__init__()
        self.client_id = client_id
        self.env = env
        self.export_path = export_path

    def on_epoch_end(self, epoch, logs=None):
        connect_remote_mlflow(self.client_id, env=ENV_SHORT_NAME)
        
        mlflow.log_metrics(
            {
                "loss": logs["loss"],
                "val_loss": logs["val_loss"],
                "val_top_010_categorical_accuracy": logs[
                    "val_factorized_top_k/top_10_categorical_accuracy"
                ],
                "val_top_050_categorical_accuracy": logs[
                    "val_factorized_top_k/top_50_categorical_accuracy"
                ],
            },
            step=epoch,
        )
