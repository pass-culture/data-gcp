import typer
from pandas import pd

app = typer.Typer()


@app.command()
def main(
    source_file_path: str = typer.Option(), output_file_path: str = typer.Option()
) -> None:
    source_df = pd.read_parquet(source_file_path)

    # Your code here
    output_df = source_df

    output_df.to_parquet(output_file_path, index=False)


if __name__ == "__main__":
    app()
