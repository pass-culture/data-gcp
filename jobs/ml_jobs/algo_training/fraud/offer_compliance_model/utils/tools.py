import time
from heapq import nlargest, nsmallest

import seaborn as sns
from loguru import logger

sns.set_theme()
sns.set(font_scale=1)


STORAGE_PATH_IMG = "./img"


def get_individual_contribution(shap_values, df_data):
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


def log_duration(message, start):
    logger.info(f"{message}: {time.time() - start} seconds.")
