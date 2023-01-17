import typer
import json

from utils.queries import get_data
from utils.constants import CONFIG_FEATURES_PATH
from utils.utils import STORAGE_PATH, ENV_SHORT_NAME


def main(
    dataset: str = typer.Option(
        f"raw_{ENV_SHORT_NAME}",
        help="BigQuery dataset in which the training table is located",
    ),
    table_name: str = typer.Option(
        "training_data_bookings",
        help="BigQuery table containing the data we want to load",
    ),
    config_file_name: str = typer.Option(
        None,
        help="Name of the config file containing feature informations",
    ),
    event_day_number: str = typer.Option(
        None,
        help="Number of days to filter when querying the data. If set to None, no filter is applied",
    ),
    limit_filter: int = typer.Option(
        None,
        help="Max number of rows",
    ),
) -> None:

    with open(
        CONFIG_FEATURES_PATH + f"/{config_file_name}.json", mode="r", encoding="utf-8"
    ) as config_file:
        features = json.load(config_file)
        columns_selected = list(features["user_embedding_layers"].keys()) + list(
            features["item_embedding_layers"].keys()
        )

    raw_data = get_data(
        dataset=dataset,
        table_name=table_name,
        columns_selected=columns_selected,
        event_day_number=event_day_number,
        max_limit=limit_filter,
    )
    raw_data.to_csv(f"{STORAGE_PATH}/raw_data.csv", index=False)


if __name__ == "__main__":
    typer.run(main)
