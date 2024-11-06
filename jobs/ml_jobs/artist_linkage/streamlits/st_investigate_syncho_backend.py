import pandas as pd
import streamlit as st

st.set_page_config(layout="wide")


@st.cache_data
def load_artist_data() -> pd.DataFrame:
    artist_df = pd.read_parquet(
        "/home/laurent_pass/Projects/data-gcp/jobs/ml_jobs/artist_linkage/streamlits/data/artist_linkage_stg_artist_table.parquet"
    )
    alias_df = pd.read_parquet(
        "/home/laurent_pass/Projects/data-gcp/jobs/ml_jobs/artist_linkage/streamlits/data/artist_linkage_stg_artist_alias_table.parquet"
    )
    product_artist_link_df = pd.read_parquet(
        "/home/laurent_pass/Projects/data-gcp/jobs/ml_jobs/artist_linkage/streamlits/data/artist_linkage_stg_product_artist_link_table.parquet"
    )

    # TODO: remove that in the tables
    artist_df = artist_df.dropna(subset=["id"])
    alias_df = alias_df.dropna(subset=["artist_id"])
    product_artist_link_df = product_artist_link_df.dropna(subset=["artist_id"])
    return artist_df, alias_df, product_artist_link_df


@st.cache_data
def load_products() -> pd.DataFrame:
    return pd.read_csv(
        "/home/laurent_pass/Téléchargements/artist_linkage_stg_tmp_artist_product.csv"
    )
    # return pd.read_parquet(
    #     "/home/laurent_pass/Téléchargements/artist_linkage_prod_tmp_artist_product.parquet"
    # ).sample(frac=0.05)


# %% Load DATA
artist_df, alias_df, product_artist_link_df = load_artist_data()
products_df = load_products()


# %% Show Dataframes
with st.expander("Artist Table", expanded=True):
    st.dataframe(artist_df)

with st.expander("Artist Alias Table", expanded=True):
    st.dataframe(alias_df)

with st.expander("Product Artist Link Table", expanded=True):
    st.dataframe(product_artist_link_df)

with st.expander("Product Table", expanded=True):
    st.dataframe(products_df)
st.divider()

# %% Do the Magic
