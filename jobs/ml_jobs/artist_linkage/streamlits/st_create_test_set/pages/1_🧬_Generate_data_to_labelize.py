import re

import pandas as pd
import streamlit as st

st.set_page_config(layout="wide")

MAX_ITEMS_IN_DATASET = 1000
ARTISTS_TO_MATCH_GCS_PATH = (
    "gs://data-bucket-prod/link_artists/artists_to_match.parquet"
)
MATCHED_ARTISTS_GCS_PATH = "gs://data-bucket-prod/link_artists/matched_artists.parquet"
TEST_SET_TO_LABELIZE_GCS_PATH = (
    "gs://data-bucket-prod/link_artists/test_sets_to_labelize"
)


# %% Load Data
@st.cache_data
def load_data() -> pd.DataFrame:
    artists_to_match_df = pd.read_parquet(ARTISTS_TO_MATCH_GCS_PATH)
    matched_artists_df = pd.read_parquet(MATCHED_ARTISTS_GCS_PATH)

    return artists_to_match_df.merge(
        matched_artists_df.drop(columns=["offer_number", "total_booking_count"]),
        on=["artist_name", "offer_category_id", "is_synchronised", "artist_type"],
        how="left",
    )


def is_snake_case(s: str) -> bool:
    snake_case_pattern = r"^[a-z]+(_[a-z]+)*$"

    return bool(re.match(snake_case_pattern, s))


def generate_test_set(input_df: pd.DataFrame):
    with st.form("Create Test Set"):
        st_columns = st.columns(4)
        with st_columns[0]:
            st_dataset_name = st.text_input("Dataset Name")
        with st_columns[1]:
            st_dataset_regex = st.text_input("Dataset Regex")
        with st_columns[2]:
            st_selected_artist_type = st.selectbox(
                "Artist Type", sorted(list(artists_df.artist_type.unique()))
            )
        with st_columns[3]:
            st_selected_category = st.selectbox(
                "Category", sorted(list(artists_df.offer_category_id.unique()))
            )

        filtered_test_set_df = (
            input_df.drop_duplicates()
            .loc[lambda df: df.artist_name.str.contains(st_dataset_regex)]
            .loc[lambda df: df.artist_type == st_selected_artist_type]
            .loc[lambda df: df.offer_category_id == st_selected_category]
            .reset_index(drop=True)
        )

        st.form_submit_button(
            "Submit",
            help="Generate Dataset. No fields must be empty",
        )

    if (
        has_errors(
            test_set_df=filtered_test_set_df,
            dataset_name=st_dataset_name,
            dataset_regex=st_dataset_regex,
        )
        is False
    ):
        file_name = f"{st_selected_artist_type}-{st_selected_category}-{st_dataset_name}.parquet".lower()

        return (
            filtered_test_set_df.assign(
                dataset_name=st_dataset_name, dataset_regex=st_dataset_regex
            ),
            file_name,
        )
    return filtered_test_set_df.iloc[:0], ""


def has_errors(test_set_df: pd.DataFrame, dataset_name, dataset_regex):
    if (not dataset_name) or (not dataset_regex):
        st.error("Dataset name and regex must be provided")
        return True
    if not is_snake_case(dataset_name):
        st.error("Dataset name should be snake_case: e.g. jean_claude_van_d")
        return True
    elif len(test_set_df) == 0:
        st.error("No items left after filtering")
        return True
    if len(test_set_df) > MAX_ITEMS_IN_DATASET:
        st.error(
            f"Too many items: {len(test_set_df)}. "
            + f"Please provide a more specific regex so we get that less than MAX_ITEMS_IN_DATASET: {MAX_ITEMS_IN_DATASET}"
        )
        return True
    return False


if __name__ == "__main__":
    artists_df = load_data()
    st.write("Number of authors", len(artists_df))
    with st.expander("Show data", expanded=False):
        st.dataframe(artists_df.sample(100))

    # Inputs to generate datasets
    test_set_df, file_name = generate_test_set(artists_df)

    # Display results
    if len(test_set_df) > 0:
        st.dataframe(test_set_df)

        # Save dataset
        file_path = f"{TEST_SET_TO_LABELIZE_GCS_PATH}/{file_name}"
        st_save_dataset_button = st.button(f"Generate Dataset : :green[{file_path}]")
        if st_save_dataset_button:
            test_set_df.to_parquet(file_path)
            st.success(f"Dataset {file_name} saved successfully")
