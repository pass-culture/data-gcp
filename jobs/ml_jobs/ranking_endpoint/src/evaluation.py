import pandas as pd
from recommenders.evaluation.python_evaluation import ndcg_at_k


def compute_ndcg_at_k(predictions: pd.DataFrame, k_list: list) -> dict:
    predictions_by_user_df = predictions.loc[
        :, ["user_id", "item_id", "target_class", "score"]
    ].drop_duplicates(subset=["user_id", "item_id"])

    rating_true_df = (
        predictions_by_user_df[
            [
                "user_id",
                "item_id",
                "target_class",
            ]
        ]
        .copy()
        .rename(
            columns={"target_class": "rating", "user_id": "userID", "item_id": "itemID"}
        )
        .assign(prediction=-1)
    )

    rating_pred_df = (
        predictions_by_user_df.assign(prediction=lambda df: df.score)[
            [
                "user_id",
                "item_id",
                "prediction",
            ]
        ]
        .copy()
        .rename(columns={"user_id": "userID", "item_id": "itemID"})
        .assign(rating=-1)
    )

    return {
        k: ndcg_at_k(
            rating_true_df,
            rating_pred_df,
            k=k,
            user_col="userID",
            item_col="itemID",
            rating_col="rating",
            prediction_col="prediction",
            relevancy_method="top_k",
            score_type="raw",
        )
        for k in k_list
    }
