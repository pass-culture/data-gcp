import pandas as pd
import streamlit as st

## Streamlit Config
st.set_page_config(layout="wide")


raw_df = pd.read_csv("streamlits/data/artist_preprocessing.csv")
with st.expander("Raw Data", expanded=False):
    st.dataframe(raw_df, width=1500, height=500)

test_set_df = (
    raw_df.sample(300, random_state=42)
    .assign(true_artist_name=lambda df: df.artist_name_to_match)
    .rename(columns={"artist_name_to_match": "predicted_artist_name"})
    .loc[
        :,
        [
            "artist_name",
            "predicted_artist_name",
            "true_artist_name",
        ],
    ]
)
st.data_editor(test_set_df, width=1500, height=500)
