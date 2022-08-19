import pandas as pd
import pandas_gbq as gbq
import numpy as np
from scipy.optimize import minimize, Bounds

from utils import STORAGE_PATH, MODEL_NAME
from tools.v1.preprocess_tools import preprocess
from tools.v2.deep_reco.preprocess_tools import lighten_matrice
from tools.v2.mf_reco.preprocess_tools import (
    get_weighted_interactions,
    group_data,
    data_prep,
    get_sparcity_filters,
    get_offers_and_users_to_keep,
    get_EAC_feedback,
)


def main():
    preprocess(STORAGE_PATH)


if __name__ == "__main__":
    main()
