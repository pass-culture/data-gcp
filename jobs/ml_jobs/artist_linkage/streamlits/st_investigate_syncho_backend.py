import pandas as pd
import streamlit as st

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
        .astype({"offer_product_id": int})
        .dropna(subset=["artist_name"])
    )


@st.cache_data
def remove_data(product_artist_link_df):
    return product_artist_link_df.sample(frac=0.95)


def count_matched_products(products_df, product_artist_link_df):
    actual_product_ids = (
        products_df.loc[:, ["offer_product_id", "artist_type"]].reset_index(drop=True)
    ).astype({"offer_product_id": int, "artist_type": str})
    linked_product_ids = product_artist_link_df.loc[
        :, ["product_id", "artist_type"]
    ].astype({"product_id": int, "artist_type": str})
    return actual_product_ids.merge(
        linked_product_ids,
        how="outer",
        left_on=["offer_product_id", "artist_type"],
        right_on=["product_id", "artist_type"],
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
with st.expander("Artist Table", expanded=True):
    st.dataframe(artist_df)

with st.expander("Artist Alias Table", expanded=True):
    st.dataframe(alias_df)

with st.expander("Product Artist Link Table", expanded=True):
    st.dataframe(raw_product_artist_link_df.sort_values(by=["product_id"]))

with st.expander("Product Table", expanded=True):
    st.dataframe(raw_products_df.sort_values(by=["offer_product_id"]))
st.divider()

# %% Check previous matching
st.subheader("Check previous matching")
merged_df = count_matched_products(raw_products_df, raw_product_artist_link_df)
st.dataframe(merged_df)
st.write(merged_df._merge.value_counts().reset_index())
st.divider()

# %% Remove data to simulate real life
products_df = raw_products_df.pipe(remove_data)
product_artist_link_df = raw_product_artist_link_df.pipe(remove_data)
st.dataframe(products_df)
st.dataframe(product_artist_link_df)

merged_df = count_matched_products(products_df, product_artist_link_df)
st.dataframe(merged_df)
st.write(merged_df._merge.value_counts().reset_index())

# %% Do the Magic
st.subheader("Split the Dataframe into the different options")
