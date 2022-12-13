import pandas as pd
from tqdm import tqdm
import numpy as np
import recmetrics
from tools.diversification import order_offers_by_score_and_diversify_categories


def get_actual_and_predicted(data_model_dict):

    data_test = data_model_dict["data"]["test"]
    data_test = data_test.sort_values(["user_id", "count"], ascending=False)
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
            "offer_subcategoryid": offer_subcategoryid.flatten().tolist(),
        }
    )
    df_predicted = df_predicted.sort_values(["score"], ascending=False)
    # return only top 50 offers since max k for metrics is 40
    return df_predicted.head(50)


def compute_metrics(data_model_dict, k):
    mark, mapk, mark_panachage, mapk_panachage = compute_recall_and_precision_at_k(
        data_model_dict, k
    )
    coverage = get_coverage_at_k(data_model_dict, k)
    avg_div_score, avg_div_score_panachage = compute_diversification_score(
        data_model_dict, k
    )
    personalization_at_k, personalization_at_k_panachage = compute_personalization(
        data_model_dict, k
    )
    data_model_dict["metrics"] = {
        "mark": mark,
        "mapk": mapk,
        "coverage": coverage,
        "mark_panachage": mark_panachage,
        "mapk_panachage": mapk_panachage,
        "personalization_at_k": personalization_at_k,
        "personalization_at_k_panachage": personalization_at_k_panachage,
        "avg_div_score": avg_div_score,
        "avg_div_score_panachage": avg_div_score_panachage,
    }
    return data_model_dict


def compute_recall_and_precision_at_k(data_model_dict, k):

    actual = data_model_dict["top_offers"].actual.values.tolist()
    model_predictions = data_model_dict["top_offers"].model_predicted.values.tolist()
    model_predictions_panachage = data_model_dict[
        "top_offers"
    ].predictions_diversified.values.tolist()
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
    catalog = data_model_dict["data"]["train"].item_id.unique().tolist()
    recos = data_model_dict["top_offers"].model_predicted.values.tolist()
    recos_at_k = []
    for reco in recos:
        recos_at_k.append(reco[:k])
    cf_coverage = recmetrics.prediction_coverage(recos_at_k, catalog)

    return cf_coverage


def compute_personalization(data_model_dict, k):
    model_predictions = data_model_dict["top_offers"].model_predicted.values.tolist()
    model_predictions_panachage = data_model_dict[
        "top_offers"
    ].predictions_diversified.values.tolist()
    personalization_at_k = get_personalization(model_predictions, k)
    personalization_at_k_panachage = get_personalization(model_predictions_panachage, k)
    return personalization_at_k, personalization_at_k_panachage


def get_personalization(model_predictions, k):
    """
    Personalization measures recommendation similarity across users.
    A high score indicates good personalization (user's lists of recommendations are different).
    A low score indicates poor personalization (user's lists of recommendations are very similar).
    """
    model_predictions_at_k = [predictions[:k] for predictions in model_predictions]
    personalization = recmetrics.personalization(predicted=model_predictions_at_k)
    return personalization


def compute_diversification_score(data_model_dict, k):
    df_raw = data_model_dict["data"]["raw"]
    recos = data_model_dict["top_offers"].model_predicted.values.tolist()
    recos_panachage = data_model_dict[
        "top_offers"
    ].predictions_diversified.values.tolist()
    avg_diversification = get_avg_diversification_score(df_raw, recos, k)
    avg_diversification_panachage = get_avg_diversification_score(
        df_raw, recos_panachage, k
    )
    return avg_diversification, avg_diversification_panachage


def get_avg_diversification_score(df_raw, recos, k):

    diversification_count = 0
    for reco in recos:
        df_enriched_recos = df_raw.query(f"item_id in {tuple(reco[:k])}")
        df_clean = df_enriched_recos[
            ["offer_categoryId", "offer_subcategoryid", "genres", "rayon", "type"]
        ]
        df_clean = df_clean.drop_duplicates()
        df_clean = df_clean.fillna("NA", inplace=False)
        count_dist = np.array(df_clean.nunique())
        diversification = np.sum(count_dist)
        diversification_count += diversification
    avg_diversification = -1
    if len(recos) > 0:
        avg_diversification = diversification_count / len(recos)
    return avg_diversification


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
