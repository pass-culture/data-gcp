import json

import pandas as pd
import polars as pl
import requests
import streamlit as st

ENVIRONMENT = "ehp"
ENV_SHORT_NAME = "stg"
PARQUET_FILE = f"gs://mlflow-bucket-{ENVIRONMENT}/streamlit_data/chatbot_edito/chatbot_encoded_offers_metadata_{ENV_SHORT_NAME}"

st.set_page_config(layout="wide")
st.title("Visualize /predict API Response")

st.sidebar.header("Request Parameters")


@st.cache_data
def load_parquet():
    print("Loading Parquet data...")
    print(f"{PARQUET_FILE}/data-sorted.parquet")
    lf = pl.scan_parquet(f"{PARQUET_FILE}/data-sorted.parquet")
    return lf


# Default payload
payload = {
    "instances": [
        {
            "search_query": "demon slayer"
            #     "filters_list": [
            #         {
            #             "operator": "=",
            #             "column": "offer_creation_date",
            #             "value": "2025-12-09",
            #         },
            # ],
        }
    ]
}
lf = load_parquet()
api_url = st.sidebar.text_input(
    "API URL",
    value="http://localhost:8085/predict",
    help="URL of the /predict endpoint",
)
payload_text = st.sidebar.text_area(
    "Request JSON payload", value=json.dumps(payload, indent=2), height=200
)


@st.cache_data(show_spinner=False)
def cached_post(api_url, payload_text):
    return requests.post(
        api_url,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        data=payload_text,
        timeout=3000,
    )


if st.sidebar.button("Send Request"):
    try:
        response = cached_post(api_url, payload_text)
        st.subheader("Raw Response")
        if response.ok:
            data = response.json()
            predictions = data.get("predictions", {})
            # Try to find offers or results in the response
            offers = predictions.get("offers")
            if offers and isinstance(offers, list):
                st.subheader("Offers Table")
                st.dataframe(pd.DataFrame(offers).head(100))
                offers = pd.DataFrame(offers)

                catalog_metadata = (
                    lf.filter(pl.col("offer_id").is_in(offers["offer_id"].tolist()))
                    .select(["offer_id", "venue_department_code"])
                    .collect()
                    .to_pandas()
                )
                print(f"Catalog metadata loaded: {len(catalog_metadata)} records")
                print(f"Merging {len(offers)} offers with metadata")
                merged_df = offers.merge(
                    catalog_metadata,
                    left_on="offer_id",
                    right_on="offer_id",
                    how="left",
                )
                st.subheader("Merged Offers with Metadata")
                st.dataframe(merged_df)
        else:
            st.info("No offers/results/items found in response.")
    except Exception as e:
        st.error(f"Request failed: {e}")
else:
    st.info("Set parameters and click 'Send Request' to visualize the response.")
