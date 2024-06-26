import mlflow
import tensorflow as tf
from utils.mlflow_tools import connect_remote_mlflow


class MatchModelCheckpoint(tf.keras.callbacks.Callback):
    def __init__(self, match_model: tf.keras.models.Model, filepath: str):
        super(MatchModelCheckpoint, self).__init__()
        self.match_model = match_model
        self.filepath = filepath

    def on_epoch_end(self, epoch, logs=None):
        tf.keras.models.save_model(self.match_model, self.filepath + "model")


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
                "Training Loss": logs.get("loss"),
                "Evaluation Loss": logs.get("val_loss"),
            },
            step=epoch,
        )

    def on_train_end(self, epoch, logs=None):
        connect_remote_mlflow(self.client_id, env=self.env)
        mlflow.log_artifacts(self.export_path + "model", "model")
