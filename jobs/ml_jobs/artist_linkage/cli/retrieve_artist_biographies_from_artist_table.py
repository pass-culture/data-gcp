import pandas as pd
import typer

from src.constants import (
    ARTIST_BIOGRAPHY_KEY,
    ARTIST_ID_KEY,
)

app = typer.Typer()


@app.command()
def main(
    applicative_artist_file_path: str = typer.Option(),
    artist_file_path: str = typer.Option(),
    output_file_path: str = typer.Option(),
) -> None:
    applicative_artists_df = pd.read_parquet(applicative_artist_file_path)
    artists_df = pd.read_parquet(artist_file_path)

    artists_df.merge(
        applicative_artists_df[[ARTIST_ID_KEY, ARTIST_BIOGRAPHY_KEY]],
        on=[ARTIST_ID_KEY],
        how="left",
    ).to_parquet(output_file_path, index=False)


if __name__ == "__main__":
    app()
