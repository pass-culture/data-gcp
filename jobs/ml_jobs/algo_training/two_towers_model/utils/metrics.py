import numpy as np
import pandas as pd
import recmetrics
from loguru import logger
from tqdm import tqdm

from commons.constants import SHUFFLE_RECOMMENDATION
from two_towers_model.utils.diversification import (
    order_offers_by_score_and_diversify_categories,
)


def get_actual_and_predicted(
    data_model_dict: dict, shuffle_recommendation: bool = SHUFFLE_RECOMMENDATION
):
    data_test = data_model_dict["data"]["test"]
    df_actual = data_test.groupby("user_id")["item_id"].agg(actual=list).reset_index()

    deep_reco_prediction = []
    predictions_diversified = []
    user_input = data_model_dict["prediction_input_feature"]

    for _, row in tqdm(
        df_actual.iterrows(), total=df_actual.shape[0], mininterval=20, maxinterval=60
    ):
        current_user = row["user_id"]
        prediction_input_feature = (
            data_test.loc[data_test["user_id"] == current_user, user_input]
            .drop_duplicates()
            .tolist()[0]
        )
        df_predicted = get_prediction(prediction_input_feature, data_model_dict)
        deep_reco_prediction.append(df_predicted["item_id"].tolist())
        diversified_prediction = order_offers_by_score_and_diversify_categories(
            df_predicted, shuffle_recommendation
        )
        predictions_diversified.append(diversified_prediction)

    data_model_dict["top_offers"] = df_actual.assign(
        model_predicted=deep_reco_prediction,
        predictions_diversified=predictions_diversified,
    )
    return data_model_dict


def get_prediction(prediction_input_feature, data_model_dict):
    model = data_model_dict["model"]
    data = data_model_dict["data"]["test"][
        ["item_id", "offer_subcategory_id"]
    ].drop_duplicates()
    nboffers = len(list(data.item_id))
    offer_to_score = np.reshape(np.array(list(data.item_id)), (nboffers, 1))
    offer_subcategory_id = np.reshape(
        np.array(list(data.offer_subcategory_id)), (nboffers,)
    )
    prediction_input = [
        np.array([prediction_input_feature] * len(offer_to_score)),
        np.array(list(data.item_id)),
    ]
    prediction = model.predict(prediction_input, verbose=0)
    df_predicted = pd.DataFrame(
        {
            "item_id": offer_to_score.flatten().tolist(),
            "score": prediction.flatten().tolist(),
            "offer_subcategory_id": offer_subcategory_id.flatten().tolist(),
        }
    )
    df_predicted = df_predicted.sort_values(["score"], ascending=False)
    # return only top 50 offers since max k for metrics is 40
    return df_predicted.head(50)


def compute_metrics(data_model_dict, k):
    try:
        logger.info("Compute recall and precision")
        mark, mapk, mark_panachage, mapk_panachage = compute_recall_and_precision_at_k(
            data_model_dict, k
        )
    except ValueError:
        mark = mapk = mark_panachage = mapk_panachage = -1

    try:
        logger.info("Compute coverage")
        coverage = get_coverage_at_k(data_model_dict, k)
    except ValueError:
        coverage = -1

    try:
        logger.info("Compute personalization score")
        personalization_at_k, personalization_at_k_panachage = compute_personalization(
            data_model_dict, k
        )
    except ValueError:
        personalization_at_k = personalization_at_k_panachage = -1

    data_model_dict["metrics"] = {
        "mark": mark,
        "mapk": mapk,
        "coverage": coverage,
        "mark_panachage": mark_panachage,
        "mapk_panachage": mapk_panachage,
        "personalization_at_k": personalization_at_k,
        "personalization_at_k_panachage": personalization_at_k_panachage,
    }
    return data_model_dict


def compute_recall_and_precision_at_k(data_model_dict, k):
    actual = data_model_dict["top_offers"]["actual"].tolist()
    model_predictions = data_model_dict["top_offers"]["model_predicted"].tolist()
    model_predictions_panachage = data_model_dict["top_offers"][
        "predictions_diversified"
    ].tolist()
    mark, mapk = get_avg_recall_and_precision_at_k(actual, model_predictions, k)
    mark_panachage, mapk_panachage = get_avg_recall_and_precision_at_k(
        actual, model_predictions_panachage, k
    )
    return mark, mapk, mark_panachage, mapk_panachage


def get_avg_recall_and_precision_at_k(actual, model_predictions, k):
    cf_mark = recmetrics.mark(actual, model_predictions, k)
    cf_mapk = mapk(actual, model_predictions, k)
    return cf_mark, cf_mapk


def get_coverage_at_k(data_model_dict, k):
    catalog = data_model_dict["data"]["training_item_ids"].tolist()
    recos = data_model_dict["top_offers"]["model_predicted"].tolist()
    recos_at_k = [reco[:k] for reco in recos]
    cf_coverage = recmetrics.prediction_coverage(recos_at_k, catalog)
    return cf_coverage


def compute_personalization(data_model_dict, k):
    model_predictions = data_model_dict["top_offers"]["model_predicted"].tolist()
    model_predictions_panachage = data_model_dict["top_offers"][
        "predictions_diversified"
    ].tolist()
    personalization_at_k = get_personalization(model_predictions, k)
    personalization_at_k_panachage = get_personalization(model_predictions_panachage, k)
    return personalization_at_k, personalization_at_k_panachage


def get_personalization(model_predictions, k):
    model_predictions_at_k = [predictions[:k] for predictions in model_predictions]
    personalization = recmetrics.personalization(predicted=model_predictions_at_k)
    return personalization


def compute_diversification_score(data_model_dict, k):
    df_raw = data_model_dict["data"]["raw"]
    recos = data_model_dict["top_offers"]["model_predicted"].tolist()
    avg_diversification = get_avg_diversification_score(df_raw, recos, k)

    recos_panachage = data_model_dict["top_offers"]["predictions_diversified"].tolist()
    avg_diversification_panachage = get_avg_diversification_score(
        df_raw, recos_panachage, k
    )
    return avg_diversification, avg_diversification_panachage


def get_avg_diversification_score(df_raw, recos, k):
    max_recos = min(10_000, len(recos))
    diversification_count = 0
    logger.info("Compute average diversification")

    # Preprocess feature data for quick lookup
    feature_columns = [
        "offer_category_id",
        "offer_subcategory_id",
        "genres",
        "rayon",
        "type",
    ]
    df_clean = df_raw[["item_id"] + feature_columns].drop_duplicates().fillna("NA")

    # Create dictionary mapping items to their unique feature sets
    item_feature_map = {}
    for item_id, group in df_clean.groupby("item_id"):
        features = {
            tuple(row)
            for row in group[feature_columns].itertuples(index=False, name=None)
        }
        item_feature_map[item_id] = features

    # Process recommendations
    for reco in tqdm(recos[:max_recos], mininterval=20, maxinterval=60):
        seen_features = set()
        for item in reco[:k]:  # Process first k items directly
            seen_features.update(item_feature_map.get(item, set()))

        if seen_features:
            # Calculate unique counts per feature column
            transposed = zip(*seen_features)
            diversification = sum(len(set(col)) for col in transposed)
            diversification_count += diversification

    avg_diversification = diversification_count / max_recos if max_recos > 0 else -1
    return avg_diversification


def apk(actual, predicted, k=10):
    if len(predicted) > k:
        predicted = predicted[:k]

    score = 0.0
    num_hits = 0.0

    for i, p in enumerate(predicted):
        if p in actual and p not in predicted[:i]:
            num_hits += 1.0
            score += num_hits / (i + 1.0)

    return score / min(len(actual), k)


def mapk(actual, predicted, k=10):
    return np.mean([apk(a, p, k) for a, p in zip(actual, predicted)])
