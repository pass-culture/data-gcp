import pandas as pd
import streamlit as st

st.set_page_config(layout="wide")


@st.cache_data
def load_data(source_file: str) -> pd.DataFrame:
    """
    Load data from a Parquet file and return it as a pandas DataFrame.

    Parameters:
        source_file (str): The path to the Parquet file.
            can be a local file or a gs:// path.

    Returns:
        pd.DataFrame: The loaded data as a pandas DataFrame.
    """
    return pd.read_parquet(source_file)


# Load data
# df = load_data("data.parquet")
input_df = pd.DataFrame(
    {
        "A": [1, 2, 3, 4],
        "B": [10, 20, 30, 40],
        "C": [100, 200, 300, 400],
    }
)


# Parameters
st.sidebar.title("Parameters")
st_selected_columns = st.sidebar.multiselect(
    options=input_df.columns.tolist(),
    label="Columns",
    default=input_df.columns.tolist()[:2],
)


# Display data
st.title("Data")
st.dataframe(input_df.loc[:, st_selected_columns])
