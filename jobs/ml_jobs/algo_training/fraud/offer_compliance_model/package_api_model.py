import json

import mlflow
import typer

from fraud.offer_compliance_model.api_model import ApiModel
from fraud.offer_compliance_model.utils.constants import CONFIGS_PATH
from utils.constants import (
    ENV_SHORT_NAME,
    MODEL_DIR,
)
from utils.mlflow_tools import connect_remote_mlflow
from utils.secrets_utils import get_secret


def package_api_model(
    model_name: str = typer.Option(
        "compliance_default", help="Model name for the training"
    ),
    config_file_name: str = typer.Option(
        ...,
        help="Name of the config file containing feature informations",
    ),
):
    with open(
        f"{MODEL_DIR}/{CONFIGS_PATH}/{config_file_name}.json",
        mode="r",
        encoding="utf-8",
    ) as config_file:
        features = json.load(config_file)

    # Connect to MLflow
    client_id = get_secret("mlflow_client_id")
    connect_remote_mlflow(client_id, env=ENV_SHORT_NAME)

    # Build the API model
    catboost_model = mlflow.catboost.load_model(
        model_uri=f"models:/{model_name}_{ENV_SHORT_NAME}/latest"
    )
    api_model = ApiModel(classification_model=catboost_model, features=features)

    # Register the API model
    client = mlflow.MlflowClient()
    run_id = client.get_latest_versions(f"{model_name}_{ENV_SHORT_NAME}")[0].run_id
    mlflow.pyfunc.log_model(
        python_model=api_model,
        artifact_path=f"registry_{ENV_SHORT_NAME}",
        registered_model_name=f"api_{model_name}_{ENV_SHORT_NAME}",
        run_id=run_id,
    )


if __name__ == "__main__":
    typer.run(package_api_model)
