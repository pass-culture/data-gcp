import numpy as np
import typer

from utils import (
    ENV_SHORT_NAME,
    create_items_table,
    get_item_docs,
    get_items_metadata,
    get_user_docs,
    get_users_dummy_metadata,
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


def main() -> None:
    prepare_docs()
    save_model_type(model_type=MODEL_TYPE)


if __name__ == "__main__":
    typer.run(main)
