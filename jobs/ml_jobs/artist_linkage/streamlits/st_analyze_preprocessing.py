# %%


import pandas as pd
import streamlit as st

from constants import ARTIST_NAME_TO_FILTER
from match_artists_on_wikidata import preprocess_artists
from utils.preprocessing_utils import (
    clean_names,
    extract_artist_name,
    extract_first_artist,
    format_names,
)

st.set_page_config(
    page_title="Scratch Analyze Preprocessing",
    page_icon=":bar_chart:",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Columns
ARTIST_ID_KEY = "artist_id"
ID_KEY = "id"
PRODUCT_ID_KEY = "offer_product_id"
ARTIST_NAME_KEY = "artist_name"
ARTIST_NAME_TO_MATCH_KEY = "artist_name_to_match"
ARTIST_TYPE_KEY = "artist_type"
OFFER_CATEGORY_ID_KEY = "offer_category_id"
ID_PER_CATEGORY = "id_per_category"

NOT_MATCHED_WITH_ARTISTS_KEY = "not_matched_with_artists"
REMOVED_PRODUCTS_KEY = "removed_products"
MATCHED_WITH_ARTISTS_KEY = "matched_with_artists"
MERGE_COLUMNS = [PRODUCT_ID_KEY, ARTIST_TYPE_KEY]


def preprocess_before_matching(df: pd.DataFrame) -> pd.DataFrame:
    return (
        (df.pipe(clean_names).pipe(extract_first_artist).pipe(format_names))
        .pipe(preprocess_artists)
        .rename(columns={"alias": ARTIST_NAME_TO_MATCH_KEY})
        .assign(artist_name_to_match=lambda df: df.artist_name_to_match.str.strip())
        .loc[lambda df: ~df.artist_name_to_match.isin(ARTIST_NAME_TO_FILTER)]
        .filter(
            [
                PRODUCT_ID_KEY,
                ARTIST_TYPE_KEY,
                OFFER_CATEGORY_ID_KEY,
                ARTIST_NAME_KEY,
                ARTIST_NAME_TO_MATCH_KEY,
                ARTIST_ID_KEY,
                "true_artist_name",
            ],
        )
    )


test_set_df = pd.read_csv(
    "/home/laurent_pass/Projects/data-gcp/jobs/ml_jobs/artist_linkage/tests/utils/data/preprocessing_test_set.csv"
)

prediction_df = (
    test_set_df.assign(old_artist_name=lambda df: df.artist_name)
    .pipe(preprocess_before_matching)
    .drop_duplicates()
).assign(
    gemini_artist_name_to_match=lambda df: df.artist_name.apply(extract_artist_name)
)


with st.expander("Show preprocessed data", expanded=False):
    st.dataframe(prediction_df)

# Results
errors_df = prediction_df.loc[lambda df: df.true_artist_name != df.artist_name_to_match]
gemini_errors_df = prediction_df.loc[
    lambda df: df.true_artist_name != df.gemini_artist_name_to_match
]
cols = st.columns([1, 1])
cols[0].markdown("### Old Script")
with cols[0].expander("Show OK", expanded=False):
    st.dataframe(
        prediction_df.loc[lambda df: df.true_artist_name == df.artist_name_to_match]
    )
cols[0].dataframe(errors_df)
cols[0].write(
    (prediction_df.true_artist_name == prediction_df.artist_name_to_match).value_counts(
        normalize=True
    )
)
cols[1].markdown("### Gemini Script")
with cols[1].expander("Show OK", expanded=False):
    st.dataframe(
        prediction_df.loc[
            lambda df: df.true_artist_name == df.gemini_artist_name_to_match
        ]
    )
cols[1].dataframe(gemini_errors_df)
cols[1].write(
    (
        prediction_df.true_artist_name == prediction_df.gemini_artist_name_to_match
    ).value_counts(normalize=True)
)
