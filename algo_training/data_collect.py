import typer

from tools.data_collect_queries import get_data
from utils import STORAGE_PATH, ENV_SHORT_NAME


def main(
    dataset: str = typer.Option(
        f"raw_{ENV_SHORT_NAME}",
        help="BigQuery dataset in which the training table is located",
    )
) -> None:
    bookings = get_data("bookings", dataset)
    bookings.to_csv(f"{STORAGE_PATH}/raw_data.csv")


if __name__ == "__main__":
    typer.run(main)
