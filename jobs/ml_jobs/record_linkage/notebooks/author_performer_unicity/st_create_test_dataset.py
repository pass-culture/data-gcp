import pandas as pd
import streamlit as st

st.set_page_config(layout="wide")

# %% Load Data
merged_df = pd.read_csv(
    "notebooks/author_performer_unicity/data/author_clustered.csv"
).sort_values(["cluster_name"])
cluster_names = [
    "NEED_CLUSTER",
    "ANOMALY_MISSING_FIRST_NAME",
    "ANOMALY_SINGLE_FIRSTNAME_LETTER",
    "ANOMALY_MULTI_AUTHOR",
    "ANOMALY_NEED_CLEANING",
] + merged_df.cluster_name.unique().tolist()
ITEM_PER_PAGE = 50
st_selected_page = st.sidebar.selectbox(
    "Page", options=[i for i in range(1, 2 + len(merged_df) // ITEM_PER_PAGE)], index=0
)

# %% Display data info
st.write("Number of authors", len(merged_df))
st.write("Number of clusters", len(cluster_names))
data = {}
page_df = merged_df.iloc[
    (st_selected_page - 1) * ITEM_PER_PAGE : st_selected_page * ITEM_PER_PAGE
]
with st.form(key="Assign Clusters"):
    for row_index, row in page_df.iterrows():
        col1, col2, _, _ = st.columns(4)
        with col1:
            st.write(row["author"])
        with col2:
            data[row_index] = st.selectbox(
                "Select the author",
                cluster_names,
                key=row_index,
                index=cluster_names.index(row["cluster_name"]),
            )
        st.write("---")

    st_button = st.form_submit_button("Submit")


if st_button is True:
    test_set_df = page_df.assign(true_cluster_name=lambda df: df.index.map(data))
    st.dataframe(test_set_df)
    test_set_df.to_csv(
        f"notebooks/author_performer_unicity/data/test_set_{st_selected_page}.csv",
        index=False,
    )
