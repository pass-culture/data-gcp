import pandas as pd
import rapidfuzz
import typer

from utils import read_parquet, upload_parquet

app = typer.Typer()


def remove_punctuation(input_df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove rows with artist names containing punctuation so that we get a clean dataset.
    Args:
        input_df: pd.DataFrame with a column 'artist_name'
    Returns: pd.DataFrame
    """
    PUNCTUATION = r"!|#|\$|\%|\&|\(|\)|\*|\+|\,|\/|\:|\;|\|\s-|\s-\s|-\s|\|"

    return (
        input_df.loc[lambda df: ~df.artist_name.str.contains(PUNCTUATION)]
        if len(input_df) > 0
        else input_df
    )


def format_artist_name(string: str) -> str:
    return " ".join(sorted(rapidfuzz.utils.default_process(string).split()))


def remove_writers_with_single_word_name(input_df: pd.DataFrame) -> pd.DataFrame:
    writers_df = (
        input_df.loc[lambda df: df.offer_category_id == "LIVRE"]
        .assign(
            preprocessed_artist_name=lambda df: df.preprocessed_artist_name.str.replace(
                r"(\b[a-zA-Z]\b)", "", regex=True
            )
            .str.replace(r"\s+", " ", regex=True)
            .str.strip()
        )
        .loc[
            lambda df: (df.preprocessed_artist_name.apply(lambda x: len(x.split())) > 1)
        ]
        .drop_duplicates()
    )
    return pd.concat(
        [input_df.loc[lambda df: df.offer_category_id != "LIVRE"], writers_df]
    )


@app.command()
def main(
    source_file_path: str = typer.Option(), output_file_path: str = typer.Option()
) -> None:
    # %%
    artists_to_match_df = read_parquet(source_file_path)

    filtered_artists_to_match_df = (
        artists_to_match_df.pipe(remove_punctuation)
        .assign(
            preprocessed_artist_name=lambda df: df.artist_name.map(format_artist_name)
        )
        .pipe(remove_writers_with_single_word_name)
    )

    upload_parquet(
        dataframe=filtered_artists_to_match_df,
        gcs_path=output_file_path,
    )


# %%
if __name__ == "__main__":
    app()
