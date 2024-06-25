import pandas as pd
import streamlit as st

## Streamlit Config
st.set_page_config(layout="wide")


## Load Data
@st.cache_data
def load_data(source_path: str) -> pd.DataFrame:
    return pd.read_parquet(source_path)


links = load_data(
    "gs://mlflow-bucket-prod/linkage_item_prod/linkage_20240625T084628/linkage_final_items_right.parquet"
)

sources = load_data(
    "gs://mlflow-bucket-prod/linkage_item_prod/linkage_20240625T084628/linkage_final_items_left.parquet"
)

## Parameters
st_link_id = st.sidebar.text_input(
    "links Link id",value="", help="Search for offer name"
)
st_batch_id = st.sidebar.text_input(
    "links batch id",value="", help="Search for offer name"
)
st_link_count= st.sidebar.number_input(
    "links link count",value=1.0, help="Search for offer name"
)
st_search_filter_links = st.sidebar.text_input(
    "links Search Filter", value="", help="Search for offer name"
)
st_search_filter_sources = st.sidebar.text_input(
    "sources Search Filter", value="", help="Search for offer name"
)



# ## Filter Dataframe
links_filtered = (
    links.loc[lambda df: df.link_id == st_link_id if st_link_id != "" else df.index]
    .loc[lambda df: df.batch_id == st_batch_id if st_batch_id != "" else df.index]
    .loc[
        lambda df: (
            df.offer_name.str.contains(st_search_filter_links, case=False)
            if st_search_filter_links != ""
            else df.index
        )
    ]
)
sources_filtered = (
    sources.loc[lambda df: df.link_id == st_link_id if st_link_id != "" else df.index ]
    .loc[lambda df: df.batch_id == st_batch_id if st_batch_id != "" else df.index]
    .loc[
        lambda df: (
            df.offer_name.str.contains(st_search_filter_sources, case=False)
            if st_search_filter_sources != ""
            else df.index
        )
    ]
)
links_clean=links.loc[lambda df: df.link_count == st_link_count if st_link_count != "" else df.index ]
# st.dataframe(sources.iloc[:100]) 
st.dataframe(links_clean)
## Display
st.header("Items linked")
# st.sidebar.write(matched_artists_df.columns)
st.dataframe(
    links_filtered.sort_values(["link_id", "link_count"]),
    width=1500,
    height=500,
)
st.header("Source Items")
st.dataframe(
    sources_filtered.sort_values(["link_id", "batch_id"]),
    width=1500,
    height=500,
)
# ## Graphs
# st.header("Metrics")
# artists_in_cluster_df = (
#     filtered_df.groupby("artist_nickname")
#     .agg({"artist_name": len})
#     .sort_values("artist_name", ascending=False)
#     .reset_index()
#     .rename(columns={"artist_name": "artist_count"})
# )
# col1, col2 = st.columns([1, 2])
# with col1:
#     st.write(artists_in_cluster_df)
# with col2:
#     percentage = (
#         100
#         * artists_in_cluster_df.loc[lambda df: df.artist_count >= 2].artist_count.sum()
#         / artists_in_cluster_df.artist_count.sum()
#     )
#     cluster_count = (
#         artists_in_cluster_df.groupby("artist_count")
#         .agg({"artist_nickname": len})
#         .reset_index()
#         .rename(columns={"artist_nickname": "cluster_count"})
#     )
#     st.subheader("Cluster Count")
#     st.bar_chart(cluster_count.set_index("artist_count"))
#     st.write(f"Artists percentage matched at least once: {percentage:.1f}%")
