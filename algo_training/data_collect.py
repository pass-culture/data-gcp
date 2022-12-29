import typer

from tools.data_collect_queries import get_data
from utils import STORAGE_PATH, ENV_SHORT_NAME


def main(
    dataset: str = typer.Option(
        f"raw_{ENV_SHORT_NAME}",
        help="BigQuery dataset in which the training table is located",
    ),
    table_name: str = typer.Option(
        "training_data_bookings",
        help="BigQuery table containing the data we want to load",
    ),
    subcategory_ids: str = typer.Option(
        None,
        help="List of subcategory ids to filter in string format. If set to None, no filter is applied",
    ),
    event_day_number: str = typer.Option(
        None,
        help="Number of days to filter when querying the data. If set to None, no filter is applied",
    ),
    limit_filter: int = typer.Option(
        -1,
        help="Max number of rows",
    ),
) -> None:
    raw_data = get_data(
        dataset, table_name, subcategory_ids, event_day_number, limit_filter
    )
    raw_data.to_csv(f"{STORAGE_PATH}/raw_data.csv", index=False)


if __name__ == "__main__":
    typer.run(main)
