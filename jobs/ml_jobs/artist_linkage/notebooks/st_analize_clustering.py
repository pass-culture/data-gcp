import pandas as pd
import streamlit as st

## Streamlit Config
st.set_page_config(layout="wide")


## Load Data
matched_artists_df = pd.read_parquet(
    "/home/laurent_pass/Téléchargements/link_artists_matched_artists.parquet"
)

## Hack
matched_artists_df = pd.concat(
    [matched_artists_df.loc[lambda df: ~df.cluster_id.isna()], matched_artists_df.loc[lambda df: df.cluster_id.isna()].assign(artist_nickname=lambda df: df.preprocessed_artist_name)]
)

## Parameters
st_artist_types = st.sidebar.multiselect(
    "Artist type",
    options=matched_artists_df.artist_type.unique(),
    default=matched_artists_df.artist_type.unique(),
)
st_category = st.sidebar.multiselect(
    "Categaroy",
    options=matched_artists_df.offer_category_id.unique(),
    default=matched_artists_df.offer_category_id.unique(),
)
st_search_filter = st.sidebar.text_input(
    "Search Filter", value="", help="Search for artist name"
)

## Filter Dataframe
filtered_df = (
    matched_artists_df.loc[lambda df: df.artist_type.isin(st_artist_types)]
    .loc[lambda df: df.offer_category_id.isin(st_category)]
    .loc[
        lambda df: (
            df.artist_name.str.contains(st_search_filter, case=False)
            if st_search_filter != ""
            else df.index
        )
    ]
)


## Display
st.sidebar.write(matched_artists_df.columns)
st.dataframe(
    filtered_df.sort_values(["cluster_id", "artist_nickname"]),
    width=1500,
    height=900,
)
