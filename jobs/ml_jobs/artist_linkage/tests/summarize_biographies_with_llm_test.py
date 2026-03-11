import pandas as pd

from cli.summarize_biographies_with_llm import merge_biographies
from src.constants import ARTIST_BIOGRAPHY_KEY, ARTIST_ID_KEY

NEW_BIO_1 = "new bio 1"
NEW_BIO_2 = "new bio 2"


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
            ARTIST_BIOGRAPHY_KEY: [NEW_BIO_1, NEW_BIO_2],
        }
    )

    result = merge_biographies(artists_df, new_biographies_df)

    assert result.loc[0, ARTIST_BIOGRAPHY_KEY] == NEW_BIO_1
    assert result.loc[1, ARTIST_BIOGRAPHY_KEY] == NEW_BIO_2


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
            ARTIST_BIOGRAPHY_KEY: [NEW_BIO_1],
        }
    )

    result = merge_biographies(artists_df, new_biographies_df)

    assert result.loc[0, ARTIST_BIOGRAPHY_KEY] == NEW_BIO_1
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
            ARTIST_BIOGRAPHY_KEY: [NEW_BIO_1],
        }
    )

    result = merge_biographies(artists_df, new_biographies_df)

    assert result.loc[0, ARTIST_BIOGRAPHY_KEY] == NEW_BIO_1
    assert pd.isna(result.loc[1, ARTIST_BIOGRAPHY_KEY])
