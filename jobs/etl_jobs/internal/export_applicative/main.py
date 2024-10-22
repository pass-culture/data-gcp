import datetime

import typer
from pandas import pd

app = typer.Typer()


@app.command()
def main(
    table_name: str = typer.Option(),
    table_schema: str = typer.Option(),
    project_id: str = typer.Option(),
    gcs_bucket: str = typer.Option(),
) -> None:
    df = pd.read_gbq(
        f"SELECT *,exclude FROM {table_schema}.{table_name};",
        project_id="my_project",
    )
    _now = datetime.today()
    yyyymmdd = _now.strftime("%Y%m%d")
    df["snapshot_date"] = yyyymmdd
    output_file_path = (
        f"gs://{gcs_bucket}/historization/applicative/{table_name}/{yyyymmdd}/*.parquet"
    )

    df.to_parquet(output_file_path, index=False)


if __name__ == "__main__":
    app()
