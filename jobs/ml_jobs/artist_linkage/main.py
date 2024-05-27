import typer
from utils import read_parquet, upload_parquet

app = typer.Typer()


@app.command()
def main(
    source_file_path: str = typer.Option(), output_file_path: str = typer.Option()
) -> None:
    df = read_parquet(source_file_path)
    upload_parquet(
        dataframe=df.assign(mock_column="mock_data"),
        gcs_path=output_file_path,
    )


if __name__ == "__main__":
    app()
