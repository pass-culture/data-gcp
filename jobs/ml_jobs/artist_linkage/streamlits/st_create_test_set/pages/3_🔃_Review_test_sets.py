import pandas as pd
import streamlit as st

from streamlits.st_create_test_set.constants import (
    TEST_SET_GCS_PATH,
    LabellingStatus,
)
from streamlits.st_create_test_set.utils import list_files

st.set_page_config(layout="wide")


def get_labelized_test_sets():
    return list_files(TEST_SET_GCS_PATH)


@st.cache_data
def load_test_set(file_name) -> pd.DataFrame:
    return pd.read_parquet(file_name)


if __name__ == "__main__":
    # Load Data
    available_test_set = get_labelized_test_sets()
    st_selected_test_set = st.sidebar.selectbox(
        "Select test set",
        options=available_test_set,
        format_func=lambda x: x.split("/")[-1].replace(".parquet", ""),
    )

    test_set = load_test_set(st_selected_test_set).sort_values(
        by=["cluster_id", "artist_name"]
    )

    # Show Data
    st.subheader("Test Set Labelized")
    st.dataframe(test_set)

    # Show Data
    st.subheader("Modify Test Set")
    with st.expander("Modify Test Set", expanded=False):
        st_selected_columns = st.multiselect(
            "Columns to Display",
            test_set.columns.tolist(),
            ["artist_name", LabellingStatus.OK.value, LabellingStatus.IRRELEVANT.value],
        )
        with st.form("Review Test Set"):
            modified_test_set = st.data_editor(
                test_set,
                column_order=st_selected_columns,
                disabled=[
                    column
                    for column in st_selected_columns
                    if column
                    not in [LabellingStatus.OK.value, LabellingStatus.IRRELEVANT.value]
                ],
            )
            st_button = st.form_submit_button("Save Changes")

        if st_button:
            st.write(modified_test_set)
            st.success("Changes saved successfully!")
