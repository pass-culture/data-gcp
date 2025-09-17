import pandas as pd
import typer

from src.utils.llm import run_writing_style_prediction

app = typer.Typer()

INPUT_FILE_PATH_OPTION = typer.Option(..., help="Path to the input file")
OUTPUT_FILE_PATH_OPTION = typer.Option(..., help="Path to the output file")
GEMINI_MODEL_NAME_OPTION = typer.Option(
    "gemini-2.5-flash", help="LLM model name to use"
)
MAX_CONCURRENT_OPTION = typer.Option(5, help="Maximum concurrent requests")
DEBUG_OPTION = typer.Option(False, help="Enable debug mode")


@app.command()
def generate_metadatas_with_llms(
    input_file_path: str = INPUT_FILE_PATH_OPTION,
    output_file_path: str = OUTPUT_FILE_PATH_OPTION,
    gemini_model_name: str = GEMINI_MODEL_NAME_OPTION,
    max_concurrent: int = MAX_CONCURRENT_OPTION,
    *,
    debug: bool = DEBUG_OPTION,
):
    """
    Extract new products from Titelive API with async LLM predictions.
    """

    raw_products_df = pd.read_parquet(input_file_path)

    # Run prediction with controlled concurrency
    output_df = run_writing_style_prediction(
        data=raw_products_df,
        gemini_model_name=gemini_model_name,
        max_concurrent=max_concurrent,
        debug=debug,
    )
    output_df.to_parquet(output_file_path, index=False)


if __name__ == "__main__":
    app()
