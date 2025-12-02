import pandas as pd
import streamlit as st

from src.constants import (
    ARTIST_ID_KEY,
    ARTIST_NAME_KEY,
    ARTIST_TYPE_KEY,
    OFFER_CATEGORY_ID_KEY,
    PRODUCT_ID_KEY,
    WIKI_ID_KEY,
)
from src.utils.gcs_utils import get_datest_from_bucket

PRODUCTS_TO_LINK_FILENAME = "products_to_link.parquet"
ARTISTS_FILENAME = "artist.parquet"
PRODUCT_ARTIST_LINK_FILENAME = "product_artist_link.parquet"
TOTAL_BOOKING_COUNT_KEY = "total_booking_count"


def get_gcs_path_from_env(env: str) -> str:
    return f"data-bucket-ml-temp-{env}/artist_linkage_{env}"


@st.cache_data
def load_data(
    st_selected_date: str,
    st_select_env: str = "prod",
):
    """Load the data from the specified file paths."""
    base_gcs_path = get_gcs_path_from_env(env=st_select_env)

    products_to_link_df = (
        pd.read_parquet(
            f"gs://{base_gcs_path}/{st_selected_date}/{PRODUCTS_TO_LINK_FILENAME}"
        )
        .astype({PRODUCT_ID_KEY: int})
        .rename(columns={ARTIST_NAME_KEY: "raw_artist_name"})
    )
    artists_df = pd.read_parquet(
        f"gs://{base_gcs_path}/{st_selected_date}/{ARTISTS_FILENAME}"
    )
    product_artist_link_df = pd.read_parquet(
        f"gs://{base_gcs_path}/{st_selected_date}/{PRODUCT_ARTIST_LINK_FILENAME}"
    ).astype({PRODUCT_ID_KEY: int})
    linked_products_df = products_to_link_df.merge(
        product_artist_link_df,
        how="left",
        on=[PRODUCT_ID_KEY, ARTIST_TYPE_KEY],
    ).merge(artists_df, how="left", on=ARTIST_ID_KEY)

    return (
        linked_products_df.loc[lambda df: df.artist_name.notna()]
        .loc[lambda df: df[TOTAL_BOOKING_COUNT_KEY] != 0]
        .sort_values(by=[TOTAL_BOOKING_COUNT_KEY], ascending=False)
        .head(10_000)
    )


def main():
    """Main function to run the Streamlit app."""
    st.set_page_config(
        page_title="Artist Linkage Analysis",
        page_icon=":bar_chart:",
        layout="wide",
    )
    st.title("Artist Linkage Analysis")

    # Params + Load
    st_select_env = st.sidebar.selectbox(
        "Select Environment",
        options=["prod", "stg", "dev"],
    )
    st_selected_date = st.sidebar.selectbox(
        "Select GCS DatePath",
        options=get_datest_from_bucket(
            gcs_path=get_gcs_path_from_env(env=st_select_env)
        ),
        index=0,
    )
    linked_products_df = load_data(
        st_selected_date=st_selected_date,
    )
    st_selected_category = st.sidebar.selectbox(
        "Select Offer Category",
        options=linked_products_df[OFFER_CATEGORY_ID_KEY].unique(),
        index=1,
    )

    # Filtering the linked products based on the selected categoryœ
    filtered_linked_products_df = linked_products_df.loc[
        lambda df: df[OFFER_CATEGORY_ID_KEY] == st_selected_category
    ]

    # Display products
    with st.expander("Linked Products DataFrame", expanded=False):
        st.dataframe(filtered_linked_products_df)

    artsts_df = (
        filtered_linked_products_df.groupby([ARTIST_ID_KEY])
        .agg(
            {
                ARTIST_NAME_KEY: "first",
                ARTIST_TYPE_KEY: "first",
                WIKI_ID_KEY: "first",
                PRODUCT_ID_KEY: "count",
                "total_booking_count": "sum",
            }
        )
        .reset_index()
        .rename(
            columns={
                PRODUCT_ID_KEY: "linked_product_count",
                "total_booking_count": "total_booking_count_sum",
            }
        )
        .sort_values(by="total_booking_count_sum", ascending=False)
    )
    st.dataframe(artsts_df)

    # Additional analysis or visualizations can be added here
    st.write("Total number of linked products:", len(filtered_linked_products_df))


if __name__ == "__main__":
    main()
