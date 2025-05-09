from datetime import datetime

import numpy as np
import typer

from utils import (
    ENV_SHORT_NAME,
    create_items_table,
    deploy_container,
    get_item_docs,
    get_items_metadata,
    get_user_docs,
    get_users_dummy_metadata,
    save_experiment,
    save_model_type,
)

MODEL_TYPE = {
    "type": "recommendation",
    "default_token": "[UNK]",
}
EMBEDDING_DIMENSION = 16


def prepare_docs():
    print("Get items...")
    items_df = get_items_metadata()
    user_df = get_users_dummy_metadata()
    # default
    user_embedding_dict = {
        row.user_id: np.random.random((EMBEDDING_DIMENSION,))
        for row in user_df.itertuples()
    }
    user_embedding_dict[MODEL_TYPE["default_token"]] = np.random.random(
        EMBEDDING_DIMENSION,
    )
    item_embedding_dict = {
        row.item_id: np.random.random((EMBEDDING_DIMENSION,))
        for row in items_df.itertuples()
    }
    user_docs = get_user_docs(user_embedding_dict)
    user_docs.save("./metadata/user.docs")
    item_docs = get_item_docs(item_embedding_dict, items_df)
    item_docs.save("./metadata/item.docs")
    create_items_table(
        item_embedding_dict,
        items_df,
        emb_size=EMBEDDING_DIMENSION,
        uri="./metadata/vector",
        create_index=True if ENV_SHORT_NAME == "prod" else False,
    )


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
    serving_container = f"europe-west1-docker.pkg.dev/passculture-infra-prod/pass-culture-artifact-registry/data-gcp/retrieval-vector/{ENV_SHORT_NAME}/{experiment_name.replace('.', '_')}:{run_id}"

    prepare_docs()
    print("Deploy...")
    save_model_type(model_type=MODEL_TYPE)
    deploy_container(serving_container, workers=2)
    save_experiment(experiment_name, model_name, serving_container, run_id=run_id)


if __name__ == "__main__":
    typer.run(main)
