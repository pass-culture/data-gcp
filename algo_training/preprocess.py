import pandas as pd
import pandas_gbq as gbq
import numpy as np
from scipy.optimize import minimize, Bounds

from utils import STORAGE_PATH
from tools.v1.preprocess_tools import preprocess


def main():
    preprocess(STORAGE_PATH)


if __name__ == "__main__":
    main()
