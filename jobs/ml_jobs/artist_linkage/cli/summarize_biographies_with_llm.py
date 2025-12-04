import pandas as pd
import typer

from src.constants import (
    ARTIST_ID_KEY,
    ARTIST_NAME_KEY,
    WIKIPEDIA_CONTENT_KEY,
)
from src.llm_config import MAX_CONCURRENT_LLM_REQUESTS
from src.utils.llm import summarize_biographies_with_llm

app = typer.Typer()


@app.command()
def main(
    artists_with_wikipedia_content: str = typer.Option(),
    output_file_path: str = typer.Option(),
    number_of_biographies_to_summarize: int = typer.Option(None),
    debug: bool = typer.Option(False),  # noqa: FBT001
) -> None:
    artists_df = pd.read_parquet(artists_with_wikipedia_content)

    # Prepare Data
    artists_to_summarize_df = artists_df.loc[
        lambda df: df[WIKIPEDIA_CONTENT_KEY].notna()
    ].loc[lambda df: df[ARTIST_NAME_KEY].notna()]

    # Predict only on few artists for testing and staging
    if number_of_biographies_to_summarize is not None:
        artists_to_summarize_df = artists_to_summarize_df.head(
            number_of_biographies_to_summarize
        )

    # Summarize biographies with LLM
    artists_with_biographies_df = summarize_biographies_with_llm(
        artists_to_summarize_df,
        max_concurrent=min(MAX_CONCURRENT_LLM_REQUESTS, len(artists_to_summarize_df)),
        debug=debug,
    )

    # Merge back the biographies to the original dataframe
    (
        artists_df.merge(
            artists_with_biographies_df,
            on=[ARTIST_ID_KEY],
            how="left",
        ).to_parquet(output_file_path, index=False)
    )


if __name__ == "__main__":
    app()
