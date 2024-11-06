import pandas as pd
import streamlit as st

DEFAULT_EXPANDER_STATE = False
MERGE_COLUMNS = ["product_id", "artist_type"]
st.set_page_config(layout="wide")


@st.cache_data
def load_artist_data() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    artist_df = pd.read_parquet(
        "/home/laurent_pass/Projects/data-gcp/jobs/ml_jobs/artist_linkage/streamlits/data/artist_linkage_stg_artist_table.parquet"
    )
    alias_df = pd.read_parquet(
        "/home/laurent_pass/Projects/data-gcp/jobs/ml_jobs/artist_linkage/streamlits/data/artist_linkage_stg_artist_alias_table.parquet"
    )
    product_artist_link_df = pd.read_parquet(
        "/home/laurent_pass/Projects/data-gcp/jobs/ml_jobs/artist_linkage/streamlits/data/artist_linkage_stg_product_artist_link_table.parquet"
    ).astype({"product_id": int})

    # TODO: remove that in the tables
    artist_df = artist_df.dropna(subset=["id"])
    alias_df = alias_df.dropna(subset=["artist_id"])
    product_artist_link_df = product_artist_link_df.dropna(subset=["artist_id"])
    return artist_df, alias_df, product_artist_link_df


@st.cache_data
def load_products() -> pd.DataFrame:
    return (
        pd.read_parquet(
            "/home/laurent_pass/Téléchargements/artist_linkage_stg_tmp_artist_product.parquet"
        )
        .rename(columns={"offer_product_id": "product_id"})
        .astype({"product_id": int})
        .dropna(subset=["artist_name"])
        .drop_duplicates(
            subset=["product_id", "artist_type"]
        )  # To remove offers with the same product_id and artist_type
    )


@st.cache_data
def remove_data(product_artist_link_df):
    return product_artist_link_df.sample(frac=0.95)


def count_matched_products(products_df, product_artist_link_df):
    actual_product_ids = products_df.loc[:, MERGE_COLUMNS].reset_index(drop=True)
    linked_product_ids = product_artist_link_df.loc[:, MERGE_COLUMNS]
    return actual_product_ids.merge(
        linked_product_ids,
        how="outer",
        left_on=MERGE_COLUMNS,
        right_on=MERGE_COLUMNS,
        indicator=True,
    ).replace(
        {
            "_merge": {
                "left_only": "Not matched with artists",
                "right_only": "Removed products",
                "both": "Matched with artists",
            }
        }
    )


#################################################################################

# %% Load DATA
st.subheader("Load Data")
artist_df, alias_df, raw_product_artist_link_df = load_artist_data()
raw_products_df = load_products()


# %% Show Dataframes
with st.expander("Artist Table", expanded=DEFAULT_EXPANDER_STATE):
    st.dataframe(artist_df)

with st.expander("Artist Alias Table", expanded=DEFAULT_EXPANDER_STATE):
    st.dataframe(alias_df)

with st.expander("Product Artist Link Table", expanded=DEFAULT_EXPANDER_STATE):
    st.dataframe(raw_product_artist_link_df.sort_values(by=["product_id"]))

with st.expander("Product Table", expanded=DEFAULT_EXPANDER_STATE):
    st.dataframe(raw_products_df.sort_values(by=["product_id"]))
st.divider()

# %% Check previous matching
st.subheader("Check matching")
with st.expander("Check matching", expanded=DEFAULT_EXPANDER_STATE):
    merged_df = count_matched_products(raw_products_df, raw_product_artist_link_df)
    st.markdown("### Before removing data")
    st.dataframe(merged_df)
    st.write(merged_df._merge.value_counts().reset_index())
    st.divider()

    # %% Remove data to simulate real life
    products_df = raw_products_df.pipe(remove_data)
    product_artist_link_df = raw_product_artist_link_df.pipe(remove_data)
    st.markdown("### After removing data")
    st.dataframe(products_df)
    st.dataframe(product_artist_link_df)

    merged_df = count_matched_products(products_df, product_artist_link_df)
    st.dataframe(merged_df)
    st.write(merged_df._merge.value_counts().reset_index())

#################################################################################

# %% Split products between to remove and to link
st.subheader("Split the Dataframe into the different options")
merged_df = count_matched_products(products_df, product_artist_link_df)

products_to_remove_df = merged_df.loc[
    lambda df: df._merge == "Removed products",
    MERGE_COLUMNS,
]
products_to_link_ids_df = merged_df.loc[
    lambda df: df._merge == "Not matched with artists",
    MERGE_COLUMNS,
]
products_to_link_df = products_to_link_ids_df.merge(
    products_df, how="left", on=MERGE_COLUMNS
)
with st.expander("Products to remove", expanded=DEFAULT_EXPANDER_STATE):
    st.write(products_to_remove_df)
with st.expander("Products to link", expanded=DEFAULT_EXPANDER_STATE):
    st.write(products_to_link_df)
