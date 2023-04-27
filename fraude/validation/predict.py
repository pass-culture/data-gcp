import shap
from heapq import nlargest, nsmallest


def get_prediction(model, pool):
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


def get_main_contribution(model, df, pool):
    explainer = shap.Explainer(model, link=shap.links.logit)
    shap_values = explainer.shap_values(pool)
    top_val, top_rej = _get_individual_contribution(shap_values, df)
    return top_val, top_rej


def _get_individual_contribution(shap_values, df_data):
    topk_validation_factor = []
    topk_rejection_factor = []
    for i in range(len(df_data)):
        individual_shap_values = list(shap_values[i, :])
        klargest = nlargest(3, individual_shap_values)
        ksmallest = nsmallest(3, individual_shap_values)
        topk_validation_factor.append(
            [
                df_data.columns[individual_shap_values.index(max_val)]
                for max_val in klargest
            ]
        )
        topk_rejection_factor.append(
            [
                df_data.columns[individual_shap_values.index(min_val)]
                for min_val in ksmallest
            ]
        )
    return topk_validation_factor, topk_rejection_factor
