import pandas as pd
import typer

from src.utils.llm import run_writing_style_prediction

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

    # TODO: Remove sample
    raw_products_df = raw_products_df.sample(30, random_state=42)
    # TODO: End TODO

    output_df = run_writing_style_prediction(
        data=raw_products_df, model_name="gemini-2.5-flash"
    )
    output_df.to_parquet(output_file_path, index=False)


if __name__ == "__main__":
    app()
