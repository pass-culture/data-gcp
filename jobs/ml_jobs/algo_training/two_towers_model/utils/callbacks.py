import mlflow
import tensorflow as tf

from commons.mlflow_tools import connect_remote_mlflow


class MLFlowLogging(tf.keras.callbacks.Callback):
    def __init__(self, export_path: str):
        super(MLFlowLogging, self).__init__()
        self.export_path = export_path
        self.total_epoch = 0
        self.total_batch = 0

    def on_batch_end(self, batch, logs=None):
        self.total_batch += 1
        mlflow.log_metrics(
            dict(
                {
                    "batch_loss": logs.get("loss", 0),
                }
            ),
            step=self.total_batch,
        )

    def on_epoch_end(self, epoch, logs=None):
        # renew http token in case train is long
        connect_remote_mlflow()
        self.total_epoch += 1
        mlflow.log_metrics(
            dict(
                {
                    "loss": logs.get("loss", 0),
                    "val_loss": logs.get("val_loss", 0),
                }
            ),
            step=self.total_epoch,
        )
