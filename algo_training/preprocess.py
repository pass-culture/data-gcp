from utils import STORAGE_PATH
from tools.v1.preprocess_tools import preprocess


def main():
    clean_data = preprocess(f"{STORAGE_PATH}/raw_data.csv").rename(
        columns={"count": "rating"}, inplace=True
    )
    clean_data.to_csv(f"{STORAGE_PATH}/clean_data.csv")


if __name__ == "__main__":
    main()
