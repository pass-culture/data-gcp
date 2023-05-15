import mlflow
import mlflow.pyfunc
from catboost import CatBoostClassifier
from sentence_transformers import SentenceTransformer
from pcpapillon.utils.env_vars import API_PARAMS, ENV_SHORT_NAME, MLFLOW_CLIENT_ID
from pcpapillon.utils.tools import connect_remote_mlflow
from pcpapillon.utils.data_model import Config

api_config = Config.from_dict(API_PARAMS)


class ModelHandler:
    def get_model_by_name(self, name):
        if name == "compliance":
            connect_remote_mlflow(MLFLOW_CLIENT_ID)
            model = CatBoostClassifier(one_hot_max_size=65)
            model_loaded = mlflow.catboost.load_model(
                model_uri=f"models:/compliance_model_{ENV_SHORT_NAME}/Production"
            )
            return model_loaded
        else:
            return SentenceTransformer(
                api_config.pre_trained_model_for_embedding_extraction[name]
            )
