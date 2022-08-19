from utils import STORAGE_PATH
from tools.data_collect_queries import get_data


def main():
    bookings = get_data("bookings")
    bookings.to_csv(f"{STORAGE_PATH}/raw_data.csv")


if __name__ == "__main__":
    main()
