from heapq import nlargest, nsmallest

import shap


def get_prediction(model, pool):
    """
    Predict validation/rejection probability for a given input as catboost pool

    inputs:
        - pool: Catboost pool with offer features
        - model: Catboost custom trained model
    outputs:
        proba_val: offer validition probability
        proba_rej: offer rejection probability (=1-proba_val)
    """
    proba_predicted = model.predict(
        pool,
        prediction_type="Probability",
        ntree_start=0,
        ntree_end=0,
        thread_count=-1,
        verbose=None,
    )
    proba_rej = [prob[0] for prob in list(proba_predicted)]
    proba_val = [prob[1] for prob in list(proba_predicted)]
    return proba_val, proba_rej


def get_prediction_main_contribution(model, data, pool):
    """
    Extract prediction main contribution features from shap values

    inputs:
        - model: Catboost custom trained model
        - data: json with offer features
        - pool: Catboost with offer features
    outputs:
        top_val: main features contributing to increase validation probability
        top_reg: main features contributing to reduce validation probability
    """
    explainer = shap.Explainer(model, link=shap.links.logit)
    shap_values = explainer.shap_values(pool)
    top_val, top_rej = _get_contribution_from_shap_values(shap_values, data)
    return top_val, top_rej


def _get_contribution_from_shap_values(shap_values, data):
    topk_validation_factor = []
    topk_rejection_factor = []
    data_keys = list(data.keys())
    # for i in range(len(data)):
    individual_shap_values = list(shap_values[0, :])
    klargest = nlargest(3, individual_shap_values)
    ksmallest = nsmallest(3, individual_shap_values)
    topk_validation_factor.append(
        [data_keys[individual_shap_values.index(max_val)] for max_val in klargest]
    )
    topk_rejection_factor.append(
        [data_keys[individual_shap_values.index(min_val)] for min_val in ksmallest]
    )
    return topk_validation_factor, topk_rejection_factor
