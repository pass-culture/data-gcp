import datetime

import pandas as pd
import typer

app = typer.Typer()


@app.command()
def run(
    table_name: str = typer.Option(),
    table_dataset: str = typer.Option(),
    project_id: str = typer.Option(),
    gcs_bucket: str = typer.Option(),
) -> None:
    df = pd.read_gbq(
        f"""SELECT * except(dbt_scd_id,dbt_updated_at,dbt_valid_from,dbt_valid_to)
        FROM {table_dataset}.{table_name};""",
        project_id=project_id,
    )
    _now = datetime.datetime.now()
    yyyymmdd = _now.strftime("%Y%m%d")
    df["import_date"] = yyyymmdd
    output_file_path = (
        f"gs://{gcs_bucket}/historization/applicative/{table_name}/{yyyymmdd}/*.parquet"
    )

    df.to_parquet(output_file_path, index=False)


if __name__ == "__main__":
    app()
