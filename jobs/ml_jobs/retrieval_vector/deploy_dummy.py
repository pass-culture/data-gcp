from datetime import datetime
import typer
from utils import (
    GCP_PROJECT_ID,
    ENV_SHORT_NAME,
    deploy_container,
    get_items_metadata,
    get_users_metadata,
    get_user_docs,
    get_item_docs,
    save_experiment,
    save_model_type,
)
import numpy as np


MODEL_TYPE = {
    "n_dim": 64,
    "metric": "cosine",
    "type": "recommendation",
    "default_token": "[UNK]",
}


def prepare_docs():
    print("Get items...")
    items_df = get_items_metadata()
    user_df = get_users_metadata()
    # default
    user_embedding_dict = {
        row.user_id: np.random.random((MODEL_TYPE["n_dim"],))
        for row in user_df.itertuples()
    }
    user_embedding_dict[MODEL_TYPE["default_token"]] = np.random.random(
        (MODEL_TYPE["n_dim"],)
    )
    item_embedding_dict = {
        row.item_id: np.random.random((MODEL_TYPE["n_dim"],))
        for row in items_df.itertuples()
    }
    user_docs = get_user_docs(user_embedding_dict)
    user_docs.save("./metadata/user.docs")
    item_docs = get_item_docs(item_embedding_dict, items_df)
    item_docs.save("./metadata/item.docs")


def main(
    experiment_name: str = typer.Option(
        None,
        help="Name of the experiment",
    ),
    model_name: str = typer.Option(
        None,
        help="Name of the model",
    ),
) -> None:

    yyyymmdd = datetime.now().strftime("%Y%m%d")
    if model_name is None:
        model_name = "default"
    run_id = f"{model_name}_{ENV_SHORT_NAME}_v{yyyymmdd}"
    serving_container = (
        f"eu.gcr.io/{GCP_PROJECT_ID}/{experiment_name.replace('.', '_')}:{run_id}"
    )
    prepare_docs()
    print("Deploy...")
    save_model_type(model_type=MODEL_TYPE)
    deploy_container(serving_container, workers=2)
    save_experiment(experiment_name, model_name, serving_container, run_id=run_id)


if __name__ == "__main__":
    typer.run(main)
