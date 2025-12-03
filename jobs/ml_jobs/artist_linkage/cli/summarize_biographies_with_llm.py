import pandas as pd
import typer

from src.constants import (
    ARTIST_ID_KEY,
    ARTIST_NAME_KEY,
    BIOGRAPHY_KEY,
    WIKIPEDIA_CONTENT_KEY,
)
from src.utils.llm import summerize_biographies_with_llm

app = typer.Typer()


@app.command()
def main(
    artists_with_wikipedia_content: str = typer.Option(),
    output_file_path: str = typer.Option(),
    max_concurrent_llm_requests: int = typer.Option(5),
) -> None:
    artists_df = pd.read_parquet(artists_with_wikipedia_content)

    # Prepare Data
    artists_to_summarize_df = artists_df.loc[
        lambda df: df[WIKIPEDIA_CONTENT_KEY].notna()
    ].loc[lambda df: df[ARTIST_NAME_KEY].notna()]

    # Summarize biographies with LLM
    artists_with_biographies_df = summerize_biographies_with_llm(
        artists_to_summarize_df,
        max_concurrent=max_concurrent_llm_requests,
    )

    # Merge back the biographies to the original dataframe
    (
        artists_df.merge(
            artists_with_biographies_df[[ARTIST_ID_KEY, BIOGRAPHY_KEY]],
            on=[ARTIST_ID_KEY],
            how="left",
        ).to_parquet(output_file_path, index=False)
    )


if __name__ == "__main__":
    app()
