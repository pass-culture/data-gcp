import pandas as pd

from src.common.constants import WIKIDATA_ID_KEY
from src.similarity.constants import MEAN_TT_ITEM_EMBEDDING_KEY

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
            return str((year // 100) + 1) + "e siècle"
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
