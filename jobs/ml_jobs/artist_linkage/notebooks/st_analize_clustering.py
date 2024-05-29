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
st_artist_types = st.sidebar.selectbox(
    "Artist type",
    options=matched_artists_df.artist_type.unique(),
    index=0
)
st_category = st.sidebar.selectbox(
    "Categary",
    options=matched_artists_df.offer_category_id.unique(),
        index=0
)
st_search_filter = st.sidebar.text_input(
    "Search Filter", value="", help="Search for artist name"
)

## Filter Dataframe
filtered_df = (
    matched_artists_df.loc[lambda df: df.artist_type == st_artist_types]
    .loc[lambda df: df.offer_category_id == st_category]
    .loc[
        lambda df: (
            df.artist_name.str.contains(st_search_filter, case=False)
            if st_search_filter != ""
            else df.index
        )
    ]
)


## Display
st.header("Matched Artists")
st.sidebar.write(matched_artists_df.columns)
st.dataframe(
    filtered_df.sort_values(["cluster_id", "artist_nickname"]),
    width=1500,
    height=500,
)


## Graphs
st.header("Metrics")
artists_in_cluster_df = filtered_df.groupby("artist_nickname").agg({"artist_name": len}).sort_values("artist_name", ascending=False).reset_index().rename(
    columns={"artist_name": "artist_count"}
)
col1, col2= st.columns([1,2])
with col1:
    st.write(artists_in_cluster_df)
with col2:
    percentage = 100 * artists_in_cluster_df.loc[lambda df: df.artist_count>=2].artist_count.sum() / artists_in_cluster_df.artist_count.sum()
    cluster_count = artists_in_cluster_df.groupby("artist_count").agg({"artist_nickname": len}).reset_index().rename(columns={"artist_nickname": "cluster_count"})
    st.subheader("Cluster Count")
    st.bar_chart(cluster_count.set_index("artist_count"))
    st.write(f"Artists percentage matched at least once: {percentage:.1f}%")