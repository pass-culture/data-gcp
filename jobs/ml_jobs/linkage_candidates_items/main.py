import typer
from utils import read_parquet, upload_parquet

app = typer.Typer()


@app.command()
def main(
    source_file_path: str = typer.Option(), output_file_path: str = typer.Option()
) -> None:
    source_df = read_parquet(source_file_path)

    # Your code here
    output_df = source_df

    upload_parquet(
        dataframe=output_df,
        gcs_path=output_file_path,
    )


if __name__ == "__main__":
    app()
