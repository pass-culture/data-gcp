import uuid

import pandas as pd
from recommenders.evaluation.python_evaluation import ndcg_at_k


def merge_wo_item_id(raw_data: pd.DataFrame, predictions: pd.DataFrame) -> pd.DataFrame:
    interactions_df = raw_data.loc[
        :,
        ["user_id", "unique_session_id"],
    ].drop_duplicates(subset="unique_session_id")

    merged_df = predictions.merge(
        interactions_df,
        on="unique_session_id",
        how="inner",
    ).drop_duplicates()

    return (
        merged_df.groupby("unique_session_id")
        .apply(
            lambda g: g.assign(
                item_id=lambda df: [uuid.uuid4() for _ in range(len(df))],
            )
        )
        .reset_index(drop=True)
    )


def compute_ndcg_at_k(
    raw_data: pd.DataFrame, predictions: pd.DataFrame, k_list: list
) -> dict:
    merged_predictions = (
        merge_wo_item_id(raw_data=raw_data, predictions=predictions)
        .assign(
            old_score=lambda df: df.score,
            new_score=lambda df: df.prob_class_consulted + 2 * df.prob_class_booked,
            retrieval_score=lambda df: df.offer_item_score,
        )
        .sort_values(
            by=["user_id", "new_score"],
            ascending=[True, False],
        )
    )
    merged_predictions[
        ["user_id", "unique_session_id", "item_id", "target_class", "new_score"]
    ]

    rating_true_df = (
        merged_predictions[
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
        merged_predictions.assign(prediction=lambda df: df.score)[
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
