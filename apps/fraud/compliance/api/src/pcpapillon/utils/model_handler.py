import mlflow
import mlflow.pyfunc
from catboost import CatBoostClassifier
from sentence_transformers import SentenceTransformer
from pcpapillon.utils.env_vars import ENV_SHORT_NAME, MLFLOW_CLIENT_ID
from pcpapillon.utils.tools import connect_remote_mlflow


class ModelHandler:
    def __init__(self, model_config):
        self.model_config = model_config

    def get_model_by_name(self, name, type="default"):
        if name == "compliance":
            if type == "local":
                model = CatBoostClassifier(one_hot_max_size=65)
                model_loaded = model.load_model("./pcpapillon/local_model/model.cb")
            else:
                connect_remote_mlflow(MLFLOW_CLIENT_ID)
                # model = CatBoostClassifier(one_hot_max_size=65)
                model_loaded = mlflow.catboost.load_model(
                    model_uri=f"models:/{name}_{type}_{ENV_SHORT_NAME}/Production"
                )
            return model_loaded
        else:
            return SentenceTransformer(
                self.model_config.pre_trained_model_for_embedding_extraction[name]
            )
