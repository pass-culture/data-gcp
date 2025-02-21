from datetime import datetime

import pandas as pd
import typer
from bulk_geocoding import geocode

from utils import chunks, query, save
from utils.constant import EXPECTED_ADRESS_COLUMNS

run = typer.Typer()


@run.command()
def user_adress(
    source_dataset_id: str = typer.Option(..., help="Source dataset id"),
    source_table_name: str = typer.Option(..., help="Source table name"),
    destination_dataset_id: str = typer.Option(..., help="Destination dataset id"),
    destination_table_name: str = typer.Option(..., help="Destination table name"),
    max_rows: int = typer.Option(1000, help="Max rows to process"),
    chunk_size: int = typer.Option(500, help="Chunk size"),
) -> None:
    df = query(source_dataset_id, source_table_name, limit=max_rows)
    if df.shape[0] == 0:
        typer.echo("No data to process")
        return

    for chunk in chunks(
        df[["user_id", "user_full_address"]].to_dict(orient="records"), chunk_size
    ):
        results = geocode(chunk, columns=["user_full_address"])
        df_results = pd.DataFrame(results, columns=EXPECTED_ADRESS_COLUMNS)
        df_results["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        save(df_results, destination_dataset_id, destination_table_name)


if __name__ == "__main__":
    run()
