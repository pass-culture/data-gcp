import pandas as pd
import streamlit as st

from streamlits.st_create_test_set.constants import (
    TEST_SET_GCS_PATH,
    TEST_SET_TO_LABELIZE_GCS_PATH,
)
from streamlits.st_create_test_set.utils import list_files

st.set_page_config(layout="wide")

COLUMNS_TO_DISPLAY = [
    "is_synchronised",
    "cluster_id",
    "offer_number",
    "total_booking_count",
    "is_multi_artists",
    "artist_name",
]


def get_test_set_to_labelize():
    return list_files(TEST_SET_TO_LABELIZE_GCS_PATH)


@st.cache_data
def load_test_sets_to_labelize(file_name) -> pd.DataFrame:
    return pd.read_parquet(file_name)


def should_match(df: pd.DataFrame):
    # return df.assign(label=lambda df: df.groupby("cluster_id").)
    return df


# %% Run Main

if __name__ == "__main__":
    # Load Data
    available_test_set = get_test_set_to_labelize()
    st_selected_test_set = st.sidebar.selectbox(
        "Select data to label",
        options=available_test_set,
        format_func=lambda x: x.split("/")[-1].replace(".parquet", ""),
    )

    test_set_to_labelize = load_test_sets_to_labelize(st_selected_test_set).sort_values(
        by=["cluster_id", "artist_name"]
    )

    # Show Data
    st.subheader("Test Set to Labelize")
    st.dataframe(test_set_to_labelize)

    # Select Artist to Labelize
    st.divider()
    st.subheader("Select Artist Nickname to Labelize")

    st_columns = st.columns([4, 1])
    with st_columns[0]:
        st.write(
            test_set_to_labelize.groupby("artist_nickname")
            .agg(
                artist_names=("artist_name", list),
                num_elements=("artist_name", "nunique"),
            )
            .sort_values(by="num_elements", ascending=False)
        )
    with st_columns[1]:
        st_artist_name = st.selectbox(
            "Select artist nickname",
            options=test_set_to_labelize.artist_nickname.unique(),
            index=None,
        )

    if st_artist_name is not None:
        st.divider()
        st.subheader("Labelize Data")
        edited_df = st.data_editor(
            test_set_to_labelize.assign(
                is_my_artist=lambda df: df.artist_nickname == st_artist_name,
                irrelevant_data=False,
            ).loc[:, COLUMNS_TO_DISPLAY + ["is_my_artist", "irrelevant_data"]],
            disabled=COLUMNS_TO_DISPLAY,
            hide_index=True,
        )

        # Save Data
        file_name = st_selected_test_set.split("/")[-1]
        file_path = f"{TEST_SET_GCS_PATH}/{file_name}"
        st_save_dataset_button = st.button(f"Generate Dataset : :green[{file_path}]")
        if st_save_dataset_button:
            final_df = test_set_to_labelize.merge(
                edited_df,
                on=COLUMNS_TO_DISPLAY,
                how="left",
            )
            st.write(final_df)
            final_df.to_parquet(file_path)
            st.success(f"Dataset {file_path} saved successfully")
