import random
from google.cloud import bigquery
import numpy as np
import pandas as pd
import tensorflow as tf


def identity_loss(y_true, y_pred):
    """Ignore y_true and return the mean of y_pred

    This is a hack to work-around the design of the Keras API that is
    not really suited to train networks with a triplet loss by default.
    """
    return tf.reduce_mean(y_pred)


def sample_triplets(pos_data, random_seed=0):
    """Sample negatives at random"""
    """Mask some user_ids at random for cold start"""
    user_ids = pos_data["user_id"].values
    pos_item_ids = pos_data["item_id"].values
    pos_subcategories = pos_data["offer_subcategoryid"].values

    client = bigquery.Client()
    query_job = client.query(
        f"""with neg_items as (
        SELECT pos_data.item_id,pos_data.offer_subcategoryid
        FROM `{GCP_PROJECT}.{BIGQUERY_CLEAN_DATASET}.temp_positive_data_train` pos_data
        ORDER BY RAND()
        LIMIT {len(user_ids)}
        )
        SELECT item_id, offer_subcategoryid from neg_items
        """
    )
    neg_items = query_job.result()
    neg_item_ids = []
    neg_subcategories = []
    for row in neg_items:
        neg_item_ids.append(row["item_id"])
        neg_subcategories.append(row["offer_subcategoryid"])

    neg_item_ids = np.array(neg_item_ids)
    neg_subcategories = np.array(neg_subcategories)
    """
    neg_item_ids = np.array(
        random.choices(pos_data["item_id"].values, k=len(user_ids)), dtype=object
    )

    neg_subcategories = (
        pd.DataFrame(neg_item_ids, columns=["item_id"])
        .set_index("item_id")
        .join(
            pos_data.drop(columns=["user_id", "click_count", "offer_id", "train_set"])
            .drop_duplicates()
            .set_index("item_id")
        )
        .offer_subcategoryid.values
    )
    """

    return [user_ids, pos_item_ids, pos_subcategories, neg_item_ids, neg_subcategories]


def predict(match_model):
    user_id = "19373"
    items_to_rank = np.array(
        ["offer-7514002", "product-2987109", "offer-6406524", "toto", "tata"]
    )
    subcat = np.array(
        [
            "BON_ACHAT_INSTRUMENT",
            "VISITE_GUIDEE",
            "ABO_BIBLIOTHEQUE",
            "EVENEMENT_CINE",
            "VISITE",
        ]
    )
    repeated_user_id = np.empty_like(items_to_rank)
    repeated_user_id.fill(user_id)
    predicted = match_model.predict(
        [repeated_user_id, items_to_rank, subcat], batch_size=4096
    )
    return predicted


def mask_random(users, proportion=0.1):
    n_users = len(users)

    mask = np.zeros(n_users)

    mask[: int(n_users * (1 - proportion))] = 1
    np.random.shuffle(mask)
    mask.astype(bool)
    users_masked = np.where(mask, users, "0")
    return users_masked
