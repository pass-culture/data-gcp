import pandas as pd

from cli.summarize_biographies_with_llm import NEW_SUFFIX
from src.constants import ARTIST_BIOGRAPHY_KEY, ARTIST_ID_KEY


def test_combine_first_prefers_new_biography_over_old():
    """New LLM-generated biographies should overwrite existing ones."""
    artists_df = pd.DataFrame(
        {
            ARTIST_ID_KEY: ["a1", "a2"],
            ARTIST_BIOGRAPHY_KEY: ["old bio 1", "old bio 2"],
        }
    )
    new_biographies_df = pd.DataFrame(
        {
            ARTIST_ID_KEY: ["a1", "a2"],
            ARTIST_BIOGRAPHY_KEY: ["new bio 1", "new bio 2"],
        }
    )

    result = (
        artists_df.merge(
            new_biographies_df,
            on=[ARTIST_ID_KEY],
            how="left",
            suffixes=("", NEW_SUFFIX),
        )
        .assign(
            **{
                ARTIST_BIOGRAPHY_KEY: lambda df: df[
                    f"{ARTIST_BIOGRAPHY_KEY}{NEW_SUFFIX}"
                ].combine_first(df[ARTIST_BIOGRAPHY_KEY])
            }
        )
        .drop(columns=[f"{ARTIST_BIOGRAPHY_KEY}{NEW_SUFFIX}"])
    )

    assert result.loc[0, ARTIST_BIOGRAPHY_KEY] == "new bio 1"
    assert result.loc[1, ARTIST_BIOGRAPHY_KEY] == "new bio 2"


def test_combine_first_keeps_old_biography_when_new_is_missing():
    """Artists not processed by LLM should keep their existing biography."""
    artists_df = pd.DataFrame(
        {
            ARTIST_ID_KEY: ["a1", "a2"],
            ARTIST_BIOGRAPHY_KEY: ["existing bio 1", "existing bio 2"],
        }
    )
    # Only a1 got a new biography
    new_biographies_df = pd.DataFrame(
        {
            ARTIST_ID_KEY: ["a1"],
            ARTIST_BIOGRAPHY_KEY: ["new bio 1"],
        }
    )

    result = (
        artists_df.merge(
            new_biographies_df,
            on=[ARTIST_ID_KEY],
            how="left",
            suffixes=("", NEW_SUFFIX),
        )
        .assign(
            **{
                ARTIST_BIOGRAPHY_KEY: lambda df: df[
                    f"{ARTIST_BIOGRAPHY_KEY}{NEW_SUFFIX}"
                ].combine_first(df[ARTIST_BIOGRAPHY_KEY])
            }
        )
        .drop(columns=[f"{ARTIST_BIOGRAPHY_KEY}{NEW_SUFFIX}"])
    )

    assert result.loc[0, ARTIST_BIOGRAPHY_KEY] == "new bio 1"
    assert result.loc[1, ARTIST_BIOGRAPHY_KEY] == "existing bio 2"


def test_combine_first_handles_no_existing_biographies():
    """Artists with no prior biography get a new one from LLM."""
    artists_df = pd.DataFrame(
        {
            ARTIST_ID_KEY: ["a1", "a2"],
            ARTIST_BIOGRAPHY_KEY: [None, None],
        }
    )
    new_biographies_df = pd.DataFrame(
        {
            ARTIST_ID_KEY: ["a1"],
            ARTIST_BIOGRAPHY_KEY: ["new bio 1"],
        }
    )

    result = (
        artists_df.merge(
            new_biographies_df,
            on=[ARTIST_ID_KEY],
            how="left",
            suffixes=("", NEW_SUFFIX),
        )
        .assign(
            **{
                ARTIST_BIOGRAPHY_KEY: lambda df: df[
                    f"{ARTIST_BIOGRAPHY_KEY}{NEW_SUFFIX}"
                ].combine_first(df[ARTIST_BIOGRAPHY_KEY])
            }
        )
        .drop(columns=[f"{ARTIST_BIOGRAPHY_KEY}{NEW_SUFFIX}"])
    )

    assert result.loc[0, ARTIST_BIOGRAPHY_KEY] == "new bio 1"
    assert pd.isna(result.loc[1, ARTIST_BIOGRAPHY_KEY])
