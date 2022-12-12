import pandas as pd

from utils import STORAGE_PATH
from tools.v1.preprocess_tools import preprocess


def main():
    raw_data = pd.read_csv(f"{STORAGE_PATH}/raw_data.csv")
    clean_data = preprocess(raw_data)
    clean_data.to_csv(f"{STORAGE_PATH}/clean_data.csv", index=False)


if __name__ == "__main__":
    main()
