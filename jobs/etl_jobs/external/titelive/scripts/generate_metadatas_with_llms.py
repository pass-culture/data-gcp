import pandas as pd
import typer

app = typer.Typer()

INPUT_FILE_PATH_OPTION = typer.Option(..., help="Path to the input file")
OUTPUT_FILE_PATH_OPTION = typer.Option(..., help="Path to the output file")


@app.command()
def generate_metadatas_with_llms(
    input_file_path: str = INPUT_FILE_PATH_OPTION,
    output_file_path: str = OUTPUT_FILE_PATH_OPTION,
):
    """
    Extract new products from Titelive API.
    """

    raw_products_df = pd.read_parquet(input_file_path)


if __name__ == "__main__":
    app()
