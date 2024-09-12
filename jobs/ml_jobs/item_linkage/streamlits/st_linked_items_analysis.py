import pandas as pd
import streamlit as st

## Streamlit Config
st.set_page_config(layout="wide")


## Load Data
@st.cache_data
def load_data(source_path: str) -> pd.DataFrame:
    return pd.read_parquet(source_path)


linked_items = load_data("./streamlits/linked_items.parquet")
linked_items = linked_items.sample(1000)
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
min_offer_name_score, max_offer_name_score = st.slider(
    "Select score range",
    min_value=float(linked_items["offer_name_score"].min()),
    max_value=float(linked_items["offer_name_score"].max()),
    value=(
        float(linked_items["offer_name_score"].min()),
        float(linked_items["offer_name_score"].max()),
    ),
)


# ## Filter Dataframe
linked_items_filtered = (
    linked_items.loc[
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
    .loc[
        lambda df: (
            df.offer_name_score >= min_offer_name_score
            if min_offer_name_score != float(linked_items["offer_name_score"].min())
            else df.index
        )
    ]
    .loc[
        lambda df: (
            df.offer_name_score <= max_offer_name_score
            if st_search_filter_linkage_candidates_items
            != float(linked_items["offer_name_score"].max())
            else df.index
        )
    ]
)


st.header(f"linked_items : {len(linked_items)} singletons matched ")
st.dataframe(
    linked_items_filtered[
        [
            "item_id_synchro",
            "offer_name_synchro",
            "offer_name_candidates",
            "item_id_candidate",
            "performer_synchro",
            "performer_candidate",
            "offer_description_synchro",
            "offer_subcategory_id_synchro",
            "offer_name_score",
        ]
    ].sort_values(["item_id_synchro"]),
    width=1500,
    height=500,
)
