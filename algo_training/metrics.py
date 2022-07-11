import numpy as np
import pandas as pd
import tensorflow as tf
from tensorflow import keras
from tqdm import tqdm
import numpy as np
import recmetrics
import matplotlib.pyplot as plt
from tools.diversification import order_offers_by_score_and_diversify_categories


def get_actual_and_predicted(data_model_dict):

    data_test = data_model_dict["data"]["test"]
    data_test = data_test.sort_values(["user_id", "rating"], ascending=False)
    df_actual = (
        data_test.copy()
        .groupby("user_id", as_index=False)["item_id"]
        .agg({"actual": (lambda x: list(x))})
    )
    deep_reco_prediction = []
    predictions_diversified = []
    for user_id in tqdm(df_actual.user_id):
        df_predicted = get_prediction(user_id, data_model_dict)
        deep_reco_prediction.append(list(df_predicted.item_id))
        # Compute diversification with score and prediction
        diversified_prediction = order_offers_by_score_and_diversify_categories(
            df_predicted
        )
        predictions_diversified.append(diversified_prediction)
    df_actual_predicted = df_actual
    df_actual_predicted["model_predicted"] = deep_reco_prediction
    df_actual_predicted["predictions_diversified"] = predictions_diversified
    data_model_dict["top_offers"] = df_actual_predicted
    return data_model_dict


def get_prediction(user_id, data_model_dict):

    model = data_model_dict["model"]
    data = data_model_dict["data"]["test"][
        ["item_id", "offer_subcategoryid"]
    ].drop_duplicates()

    nboffers = len(list(data.item_id))
    offer_to_score = np.reshape(np.array(list(data.item_id)), (nboffers, 1))
    user_to_rank = np.reshape(
        np.array([str(user_id)] * len(offer_to_score)), (nboffers, 1)
    )
    offer_subcategoryid = np.reshape(
        np.array(list(data.offer_subcategoryid)), (nboffers, 1)
    )
    pred_input = [user_to_rank, offer_to_score]
    prediction = model.predict(pred_input, verbose=0)
    df_predicted = pd.DataFrame(
        {
            "item_id": offer_to_score.flatten().tolist(),
            "score": prediction.flatten().tolist(),
            "subcategory": offer_subcategoryid.flatten().tolist(),
        }
    )
    df_predicted = df_predicted.sort_values(["score"], ascending=False)
    return df_predicted


def compute_metrics(data_model_dict, k):
    mark, mapk, div_mark, div_mapk = compute_recall_and_precision_at_k(
        data_model_dict, k
    )
    coverage = get_coverage_at_k(data_model_dict, k)
    data_model_dict["metrics"] = {
        "mark": mark,
        "mapk": mapk,
        "coverage": coverage,
        "div_mark": div_mark,
        "div_mapk": div_mapk,
    }
    return data_model_dict


def compute_recall_and_precision_at_k(data_model_dict, k):

    actual = data_model_dict["top_offers"].actual.values.tolist()
    model_predictions = data_model_dict["top_offers"].model_predicted.values.tolist()
    model_predictions_diversified = data_model_dict[
        "top_offers"
    ].predictions_diversified.values.tolist()
    mark, mapk = get_avg_recall_and_precision_at_k(actual, model_predictions, k)
    div_mark, div_mapk = get_avg_recall_and_precision_at_k(
        actual, model_predictions_diversified, k
    )

    return mark, mapk, div_mark, div_mapk


def get_avg_recall_and_precision_at_k(actual, model_predictions, k):

    cf_mark = recmetrics.mark(actual, model_predictions, k)
    cf_mapk = mapk(actual, model_predictions, k)
    return cf_mark, cf_mapk


def get_coverage_at_k(data_model_dict, k):
    catalog = data_model_dict["data"]["train"].item_id.unique().tolist()
    recos = data_model_dict["top_offers"].model_predicted.values.tolist()
    recos_at_k = []
    for reco in recos:
        recos_at_k.append(reco[:k])
    cf_coverage = recmetrics.prediction_coverage(recos_at_k, catalog)

    return cf_coverage


def apk(actual, predicted, k=10):
    """
    Computes the average precision at k.

    This function computes the average prescision at k between two lists of
    items.

    Parameters
    ----------
    actual : list
            A list of elements that are to be predicted (order doesn't matter)
    predicted : list
                A list of predicted elements (order does matter)
    k : int, optional
        The maximum number of predicted elements

    Returns
    -------
    score : double
            The average precision at k over the input lists

    """
    if len(predicted) > k:
        predicted = predicted[:k]

    score = 0.0
    num_hits = 0.0

    for i, p in enumerate(predicted):
        if p in actual and p not in predicted[:i]:
            num_hits += 1.0
            score += num_hits / (i + 1.0)

    if not actual:
        return 0.0

    return score / min(len(actual), k)


def mapk(actual, predicted, k=10):
    """
    Computes the mean average precision at k.

    This function computes the mean average prescision at k between two lists
    of lists of items.

    Parameters
    ----------
    actual : list
            A list of lists of elements that are to be predicted
            (order doesn't matter in the lists)
    predicted : list
                A list of lists of predicted elements
                (order matters in the lists)
    k : int, optional
        The maximum number of predicted elements

    Returns
    -------
    score : double
            The mean average precision at k over the input lists

    """
    return np.mean([apk(a, p, k) for a, p in zip(actual, predicted)])
