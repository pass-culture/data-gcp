import pandas as pd
import streamlit as st

st.set_page_config(layout="wide")

# %% Load Data
merged_df = pd.read_csv(
    "notebooks/author_performer_unicity/data/author_clustered.csv"
).sort_values(["cluster_name"])

# %% Display the results
cluster_names = ["NO CLUSTER"] + merged_df.cluster_name.unique().tolist()
st.write("Number of authors", len(merged_df))
st.write("Number of clusters", len(cluster_names))
data = {}
with st.form(key="Assign Clusters"):
    for row_index, row in merged_df.iterrows():
        col1, col2 = st.columns(2)
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
    test_set_df = merged_df.assign(true_cluster_name=lambda df: df.index.map(data))
    st.dataframe(test_set_df)
    test_set_df.to_csv(
        "notebooks/author_performer_unicity/data/test_set.csv", index=False
    )
