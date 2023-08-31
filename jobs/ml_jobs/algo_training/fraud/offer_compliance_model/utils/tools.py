import os
import shutil
import urllib.request
from heapq import nlargest, nsmallest

import numpy as np
import pandas as pd
from catboost import Pool
from PIL import Image
from sentence_transformers import SentenceTransformer
from tqdm import tqdm

import seaborn as sns

sns.set_theme()
sns.set(font_scale=1)

import os
import time
from multiprocessing import cpu_count
import concurrent
from itertools import repeat
import mlflow
from loguru import logger

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
