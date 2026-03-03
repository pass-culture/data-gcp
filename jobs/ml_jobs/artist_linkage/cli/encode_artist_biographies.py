import pandas as pd
import typer
from sentence_transformers import SentenceTransformer

from src.constants import (
    HF_TOKEN_SECRET_NAME,
    MEAN_TT_ITEM_EMBEDDING_KEY,
    WIKIDATA_ID_KEY,
)
from src.utils.gcp import get_secret
from src.utils.loading import load_wikidata

app = typer.Typer()

GENRES_KEY = "genres"
PROFESSIONS_KEY = "professions"
LANGUAGES_SPOKEN_KEY = "languages_spoken"
BIRTH_DATE_KEY = "birth_date_val"


def format_wikidata_series(series: pd.Series, title: str) -> pd.Series:
    return series.where(
        series.isna() | series.eq(""),
        series.astype(str).radd(title + ": ").add("\n"),
    )


def map_birth_date_to_epoch(birth_date: str) -> str | None:
    if pd.isna(birth_date):
        return None

    try:
        date_part = birth_date.split("T")[0]

        if date_part.startswith("-"):
            # Negative year (e.g. -2304-01-01)
            parts = date_part.split("-")
            year = -int(parts[1])
        else:
            # Positive year (e.g. 1955-03-04)
            parts = date_part.split("-")
            year_str = parts[0]
            if year_str.startswith("+"):
                year_str = year_str[1:]
            year = int(year_str)

        if year < 1900:
            # Century for years < 1900 (e.g. 1844 -> 1800, -2304 -> -2400)
            return str(year // 100) + "e siècle"
        else:
            # Decade for years >= 1900 (e.g. 1944 -> 1940)
            return f"{(year // 10) * 10}s"

    except (ValueError, IndexError):
        return None


def get_enriched_artist_df(
    artist_df: pd.DataFrame, wiki_df: pd.DataFrame
) -> pd.DataFrame:
    return (
        artist_df.drop_duplicates(
            subset=artist_df.columns.difference(
                [MEAN_TT_ITEM_EMBEDDING_KEY]
            )  # Cannot drop duplicates on the MEAN_TT_ITEM_EMBEDDING_KEY column as it is an array, which is unhashable
        )
        .merge(wiki_df, on=WIKIDATA_ID_KEY, how="left")
        .assign(
            genres=lambda df: format_wikidata_series(
                series=df[GENRES_KEY], title="genre(s)"
            ),
            professions=lambda df: format_wikidata_series(
                series=df[PROFESSIONS_KEY], title="profession(s)"
            ),
            languages_spoken=lambda df: format_wikidata_series(
                series=df[LANGUAGES_SPOKEN_KEY], title="langue parlee(s)"
            ),
            decennie_date=lambda df: format_wikidata_series(
                series=df[BIRTH_DATE_KEY].map(map_birth_date_to_epoch),
                title="date de naissance",
            ),
            biography=lambda df: format_wikidata_series(
                series=df["artist_biography"], title="biographie"
            ),
            enriched_artist_biography=lambda df: (
                df["genres"].fillna("")
                + df["professions"].fillna("")
                + df["languages_spoken"].fillna("")
                + df["decennie_date"].fillna("")
                + df["biography"].fillna("")
            ),
        )
        .reset_index(drop=True)
    )


def embed_artist_biographies(artist_biographies: pd.Series) -> pd.Series:
    gemma_encoder = SentenceTransformer(
        "google/embeddinggemma-300m",
        token=get_secret(HF_TOKEN_SECRET_NAME),
    )
    PROMPT_NAME = "Clustering"
    BATCH_SIZE = 128

    embeddings = gemma_encoder.encode(
        artist_biographies.tolist(),
        show_progress_bar=True,
        batch_size=BATCH_SIZE,
        prompt_name=PROMPT_NAME,
    )
    return pd.Series(list(embeddings), index=artist_biographies.index)


@app.command()
def main(
    artist_with_biography_file_path: str = typer.Option(),
    wiki_base_path: str = typer.Option(),
    wiki_file_name: str = typer.Option(),
    output_file_path: str = typer.Option(),
) -> None:
    artists_df = pd.read_parquet(artist_with_biography_file_path)
    wikidata_df = (
        load_wikidata(wiki_base_path, wiki_file_name)
        .loc[
            :,
            [
                WIKIDATA_ID_KEY,
                GENRES_KEY,
                PROFESSIONS_KEY,
                LANGUAGES_SPOKEN_KEY,
                BIRTH_DATE_KEY,
            ],
        ]
        .drop_duplicates()
        .reset_index(drop=True)
    )

    # Enrich artist biographies with Wikidata information
    enriched_artist_df = get_enriched_artist_df(artists_df, wikidata_df)

    # Encode enriched artist biographies
    enriched_artist_df.assign(
        semantic_embedding=lambda df: embed_artist_biographies(
            df.enriched_artist_biography
        )
    ).to_parquet(output_file_path, index=False)


if __name__ == "__main__":
    app()
