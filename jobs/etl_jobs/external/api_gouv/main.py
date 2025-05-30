from datetime import datetime

import pandas as pd
import typer

from utils import chunks, query, save
from utils.constant import GEOPF_EXPECTED_ADDRESS_COLUMNS as EXPECTED_ADDRESS_COLUMNS
from utils.geocoding import AddressGeocoder

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

    print(f"Processing {df.shape[0]} rows... ")
    geocoder = AddressGeocoder()
    results = []
    for chunk in chunks(
        df[["user_id", "user_full_address"]].to_dict(orient="records"), chunk_size
    ):
        r = geocoder.geocode_batch(
            addresses=chunk, address_columns=["user_full_address"]
        )
        results.extend(r)
        print(f"Processed {len(r)} rows... ")

    df_results = pd.DataFrame(results, columns=EXPECTED_ADDRESS_COLUMNS)
    df_results["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"Saving {df_results.shape[0]} rows... ")
    save(df_results, destination_dataset_id, destination_table_name)

    print(f"Done processing {df.shape[0]} rows")


if __name__ == "__main__":
    run()
