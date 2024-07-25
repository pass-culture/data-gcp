import pandas as pd
import streamlit as st

## Streamlit Config
st.set_page_config(layout="wide")


## Load Data
@st.cache_data
def load_data(source_path: str) -> pd.DataFrame:
    return pd.read_parquet(source_path)


linkage_j_70 = load_data("./streamlits/linkage_final_items_jaro_70.parquet")

linkage_j_80 = load_data("./streamlits/linkage_final_items_jaro_80.parquet")

linkage_j_90 = load_data("./streamlits/linkage_final_items_jaro_90.parquet")

linkage_jw_90 = load_data("./streamlits/linkage_final_items_jarowinkler_90.parquet")

linkage_lev_80 = load_data("./streamlits/linkage_final_items_lev_80.parquet")

linkage_lev_90 = load_data("./streamlits/linkage_final_items_lev_90.parquet")
# linkage_candidates_items = load_data(
#     "gs://mlflow-bucket-prod/linkage_item_prod/linkage_20240722T161954/linkage_candidates_items.parquet"
# )

## Parameters
st_link_id = st.sidebar.text_input(
    "linkage Link id", value="", help="Search for offer name"
)
st_item_id_synchro = st.sidebar.text_input(
    "synchro item_id", value="", help="Search for offer name"
)
st_item_id_singleton = st.sidebar.text_input(
    "singleton item_id", value="", help="Search for offer name"
)
st_search_filter_linkage = st.sidebar.text_input(
    "linkage Search Filter", value="", help="Search for offer name"
)
st_search_filter_linkage_candidates_items = st.sidebar.text_input(
    "linkage_candidates_items Search Filter", value="", help="Search for offer name"
)


# ## Filter Dataframe
linkage_filtered_j_70 = (
    linkage_j_70.loc[
        lambda df: df.link_id == st_link_id if st_link_id != "" else df.index
    ]
    .loc[
        lambda df: df.item_id_synchro == st_item_id_synchro
        if st_item_id_synchro != ""
        else df.index
    ]
    .loc[
        lambda df: df.item_id_candidate == st_item_id_singleton
        if st_item_id_singleton != ""
        else df.index
    ]
    .loc[
        lambda df: (
            df.offer_name_synchro.str.contains(st_search_filter_linkage, case=False)
            if st_search_filter_linkage != ""
            else df.index
        )
    ]
    .loc[
        lambda df: (
            df.offer_name_candidates.str.contains(
                st_search_filter_linkage_candidates_items, case=False
            )
            if st_search_filter_linkage_candidates_items != ""
            else df.index
        )
    ]
)


linkage_filtered_j_80 = (
    linkage_j_80.loc[
        lambda df: df.link_id == st_link_id if st_link_id != "" else df.index
    ]
    .loc[
        lambda df: df.item_id_synchro == st_item_id_synchro
        if st_item_id_synchro != ""
        else df.index
    ]
    .loc[
        lambda df: df.item_id_candidate == st_item_id_singleton
        if st_item_id_singleton != ""
        else df.index
    ]
    .loc[
        lambda df: (
            df.offer_name_synchro.str.contains(st_search_filter_linkage, case=False)
            if st_search_filter_linkage != ""
            else df.index
        )
    ]
    .loc[
        lambda df: (
            df.offer_name_candidates.str.contains(
                st_search_filter_linkage_candidates_items, case=False
            )
            if st_search_filter_linkage_candidates_items != ""
            else df.index
        )
    ]
)

linkage_filtered_j_90 = (
    linkage_j_90.loc[
        lambda df: df.link_id == st_link_id if st_link_id != "" else df.index
    ]
    .loc[
        lambda df: df.item_id_synchro == st_item_id_synchro
        if st_item_id_synchro != ""
        else df.index
    ]
    .loc[
        lambda df: df.item_id_candidate == st_item_id_singleton
        if st_item_id_singleton != ""
        else df.index
    ]
    .loc[
        lambda df: (
            df.offer_name_synchro.str.contains(st_search_filter_linkage, case=False)
            if st_search_filter_linkage != ""
            else df.index
        )
    ]
    .loc[
        lambda df: (
            df.offer_name_candidates.str.contains(
                st_search_filter_linkage_candidates_items, case=False
            )
            if st_search_filter_linkage_candidates_items != ""
            else df.index
        )
    ]
)

linkage_filtered_jw_90 = (
    linkage_jw_90.loc[
        lambda df: df.link_id == st_link_id if st_link_id != "" else df.index
    ]
    .loc[
        lambda df: df.item_id_synchro == st_item_id_synchro
        if st_item_id_synchro != ""
        else df.index
    ]
    .loc[
        lambda df: df.item_id_candidate == st_item_id_singleton
        if st_item_id_singleton != ""
        else df.index
    ]
    .loc[
        lambda df: (
            df.offer_name_synchro.str.contains(st_search_filter_linkage, case=False)
            if st_search_filter_linkage != ""
            else df.index
        )
    ]
    .loc[
        lambda df: (
            df.offer_name_candidates.str.contains(
                st_search_filter_linkage_candidates_items, case=False
            )
            if st_search_filter_linkage_candidates_items != ""
            else df.index
        )
    ]
)

linkage_filtered_lev_80 = (
    linkage_lev_80.loc[
        lambda df: df.link_id == st_link_id if st_link_id != "" else df.index
    ]
    .loc[
        lambda df: df.item_id_synchro == st_item_id_synchro
        if st_item_id_synchro != ""
        else df.index
    ]
    .loc[
        lambda df: df.item_id_candidate == st_item_id_singleton
        if st_item_id_singleton != ""
        else df.index
    ]
    .loc[
        lambda df: (
            df.offer_name_synchro.str.contains(st_search_filter_linkage, case=False)
            if st_search_filter_linkage != ""
            else df.index
        )
    ]
    .loc[
        lambda df: (
            df.offer_name_candidates.str.contains(
                st_search_filter_linkage_candidates_items, case=False
            )
            if st_search_filter_linkage_candidates_items != ""
            else df.index
        )
    ]
)

linkage_filtered_lev_90 = (
    linkage_lev_90.loc[
        lambda df: df.link_id == st_link_id if st_link_id != "" else df.index
    ]
    .loc[
        lambda df: df.item_id_synchro == st_item_id_synchro
        if st_item_id_synchro != ""
        else df.index
    ]
    .loc[
        lambda df: df.item_id_candidate == st_item_id_singleton
        if st_item_id_singleton != ""
        else df.index
    ]
    .loc[
        lambda df: (
            df.offer_name_synchro.str.contains(st_search_filter_linkage, case=False)
            if st_search_filter_linkage != ""
            else df.index
        )
    ]
    .loc[
        lambda df: (
            df.offer_name_candidates.str.contains(
                st_search_filter_linkage_candidates_items, case=False
            )
            if st_search_filter_linkage_candidates_items != ""
            else df.index
        )
    ]
)

# linkage_candidates_items_filtered = (
#     linkage_candidates_items.loc[lambda df: df.link_id == st_link_id if st_link_id != "" else df.index]
#     .loc[
#         lambda df: df.item_id_synchro == st_item_id_synchro
#         if st_item_id_synchro != ""
#         else df.index
#     ]
#     .loc[
#         lambda df: df.item_id_candidate == st_item_id_singleton
#         if st_item_id_singleton != ""
#         else df.index
#     ]
#     .loc[
#         lambda df: (
#             df.offer_name.str.contains(st_search_filter_linkage, case=False)
#             if st_search_filter_linkage != ""
#             else df.index
#         )
#     ]
# )

# st.header(f"linkage_candidates_items_filtered")
# st.dataframe(
#     linkage_candidates_items_filtered[["item_id_synchro","offer_name_synchro","offer_name_candidates","item_id_candidate","performer_synchro","performer_candidate","offer_description_synchro","offer_subcategory_id_synchro"]].sort_values(["item_id_synchro"]),
#     width=1500,
#     height=500,
# )

st.header(f"Jaro / 0.70 : {len(linkage_j_70)} singletons matched ")
st.dataframe(
    linkage_filtered_j_70[
        [
            "item_id_synchro",
            "offer_name_synchro",
            "offer_name_candidates",
            "item_id_candidate",
            "performer_synchro",
            "performer_candidate",
            "offer_description_synchro",
            "offer_subcategory_id_synchro",
        ]
    ].sort_values(["item_id_synchro"]),
    width=1500,
    height=500,
)

st.header(f"Jaro / 0.80 : {len(linkage_j_80)} singletons matched")
st.dataframe(
    linkage_filtered_j_80[
        [
            "item_id_synchro",
            "offer_name_synchro",
            "offer_name_candidates",
            "item_id_candidate",
            "performer_synchro",
            "performer_candidate",
            "offer_description_synchro",
            "offer_subcategory_id_synchro",
        ]
    ].sort_values(["item_id_synchro"]),
    width=1500,
    height=500,
)

st.header(f"Jaro / 0.90 : {len(linkage_j_90)} singletons matched")
st.dataframe(
    linkage_filtered_j_90[
        [
            "item_id_synchro",
            "offer_name_synchro",
            "offer_name_candidates",
            "item_id_candidate",
            "performer_synchro",
            "performer_candidate",
            "offer_description_synchro",
            "offer_subcategory_id_synchro",
        ]
    ].sort_values(["item_id_synchro"]),
    width=1500,
    height=500,
)

st.header(f"Jaro-winkler / 0.90 : {len(linkage_jw_90)} ")
st.dataframe(
    linkage_filtered_jw_90[
        [
            "item_id_synchro",
            "offer_name_synchro",
            "offer_name_candidates",
            "item_id_candidate",
            "performer_synchro",
            "performer_candidate",
            "offer_description_synchro",
            "offer_subcategory_id_synchro",
        ]
    ].sort_values(["item_id_synchro"]),
    width=1500,
    height=500,
)

st.header(f"Levenshtein / 0.80 : {len(linkage_lev_80)} singletons matched")
st.dataframe(
    linkage_filtered_lev_80[
        [
            "item_id_synchro",
            "offer_name_synchro",
            "offer_name_candidates",
            "item_id_candidate",
            "performer_synchro",
            "performer_candidate",
            "offer_description_synchro",
            "offer_subcategory_id_synchro",
        ]
    ].sort_values(["item_id_synchro"]),
    width=1500,
    height=500,
)

st.header(f"Levenshtein / 0.90 : {len(linkage_lev_90)} singletons matched")
st.dataframe(
    linkage_filtered_lev_90[
        [
            "item_id_synchro",
            "offer_name_synchro",
            "offer_name_candidates",
            "item_id_candidate",
            "performer_synchro",
            "performer_candidate",
            "offer_description_synchro",
            "offer_subcategory_id_synchro",
        ]
    ].sort_values(["item_id_synchro"]),
    width=1500,
    height=500,
)
