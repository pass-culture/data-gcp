import time

import streamlit as st

import pandas as pd
from collections import defaultdict

from scipy.cluster.hierarchy import linkage, fcluster
from scipy.spatial.distance import squareform
from sklearn.cluster import AffinityPropagation

import rapidfuzz
import stqdm

st.set_page_config(layout="wide")

# %% Load Data
author_df = pd.read_csv("notebooks/author_performer_unicity/data/author.csv")
CATEGORIES = author_df.offer_category_id.unique()
PUNCTUATION = r"!|#|\$|\%|\&|\(|\)|\*|\+|\,|\/|\:|\;|\|\s-|\s-\s|-\s|\|"  # '<=>?@[\\]^_`{|}~|\s–\s'

# %%
selected_category = st.sidebar.selectbox(
    "category", options=CATEGORIES, index=0  # len(CATEGORIES) - 1
)
only_synchronized = st.sidebar.checkbox("only synchronized", value=False)
only_booked = st.sidebar.checkbox("only booked", value=False)
selected_punctuation = st.sidebar.selectbox(
    "only with punctuation", options=["WITHOUT", "WITH", "NEVERMIND"]
)
search_filter = st.sidebar.text_input("search", value="oda")

category_df = author_df.dropna().loc[
    lambda df: df.offer_category_id == selected_category
]
filtered_df = (
    category_df.loc[lambda df: df.is_synchronised if only_synchronized else df.index]
    .loc[lambda df: df.booking_cnt > 0 if only_booked else df.index]
    .loc[
        lambda df: (
            df.author.str.contains(PUNCTUATION)
            if selected_punctuation == "WITH"
            else (
                ~df.author.str.contains(PUNCTUATION)
                if selected_punctuation == "WITHOUT"
                else df.index
            )
        )
    ]
    .loc[
        lambda df: (
            df.author.str.contains(search_filter) if search_filter != "" else df.index
        )
    ]
)

# %% Print samples
st.write("Number of authors", len(category_df))
st.write("Number of authors after filtering", len(filtered_df))
st.progress(len(filtered_df) / len(category_df))

st.markdown("""---""")
if len(filtered_df) > 0:
    st.dataframe(
        filtered_df.loc[:, ["author", "booking_cnt"]],
        width=1500,
        height=1500,
        hide_index=True,
    )


# %% Clusterisation
# Function to group similar artist names


def preprocessing(string: str) -> str:
    return " ".join(sorted(rapidfuzz.utils.default_process(string).split()))


def ratio(string_a: str, string_b: str, method: str) -> float:

    if method == "ratio":
        return rapidfuzz.fuzz.token_sort_ratio(string_a, string_b) / 100
    if method == "damerau-levenshtein":
        return rapidfuzz.distance.DamerauLevenshtein.normalized_similarity(
            string_a, string_b
        )
    if method == "jaro-winkler":
        return rapidfuzz.distance.JaroWinkler.normalized_similarity(string_a, string_b)
    if method == "lcs-seq":
        return rapidfuzz.distance.LCSseq.normalized_similarity(string_a, string_b)
    if method == "osa":
        return rapidfuzz.distance.OSA.normalized_similarity(string_a, string_b)


def group_artist_names(names, threshold, method):
    grouped_names = defaultdict(list)
    seen_names = set()

    for name in stqdm.stqdm(names):
        # Check if the name is already seen or grouped
        if name.lower() not in seen_names:
            for grouped_name in grouped_names:

                if ratio(name, grouped_name, method) > threshold:
                    grouped_names[grouped_name].append(name)
                    seen_names.add(name.lower())
                    break

            else:
                grouped_names[name] = [name]
                seen_names.add(name.lower())

    return list(grouped_names.values())


# Group similar artist names
st.markdown("""---""")
st.header("Calculating Clusters")
with st.form("compute clusters"):
    st_threshold = st.slider(
        "Levenstein threshold",
        min_value=0.0,
        max_value=1.0,
        step=0.01,
        value=0.9,
    )
    st_method = st.selectbox(
        "method",
        options=["ratio", "jaro-winkler", "damerau-levenshtein", "lcs-seq", "osa"],
        index=4,
    )
    st_cluster_threshold = st.slider(
        "Clustering threshold",
        min_value=0.0,
        max_value=1.0,
        step=0.01,
        value=0.2,
    )
    authors_list = filtered_df.author.map(preprocessing).drop_duplicates().tolist()
    st.write("Number of authors", len(authors_list))
    # Every form must have a submit button.
    submitted = st.form_submit_button("Compute")

    if submitted is True:

        t0 = time.time()
        grouped_names = group_artist_names(authors_list, st_threshold, st_method)
        matched_authors = (
            pd.DataFrame({"author": grouped_names})
            .assign(num_authors=lambda df: df.author.apply(len))
            .sort_values("num_authors", ascending=False)
        )
        st.write("Time to compute the clusters", time.time() - t0)
        st.write("Number of clusters", len(matched_authors))
        st.write("Number of authors", matched_authors.num_authors.sum())
        st.dataframe(matched_authors, width=1500)

        # Compute the parwise distance between the authors
        t0 = time.time()
        distance_matrix = rapidfuzz.process.cdist(
            queries=authors_list,
            choices=authors_list,
            scorer=rapidfuzz.distance.OSA.normalized_distance,
        )
        st.write("Time to compute the matrix", time.time() - t0)

        # Perform hierarchical/agglomerative clustering
        condensed_dist_matrix = squareform(distance_matrix)
        Z = linkage(condensed_dist_matrix, "centroid")

        t0 = time.time()
        clusters = fcluster(Z, st_cluster_threshold, criterion="distance")

        # Show the clusters
        clusters_df = (
            pd.DataFrame({"author": authors_list})
            .assign(cluster=clusters)
            .groupby("cluster")
            .agg({"author": set})
            .assign(num_authors=lambda df: df.author.map(len))
            .sort_values("num_authors", ascending=False)
        )
        st.dataframe(
            clusters_df,
            width=1500,
        )

        st.write("Time to compute the clusters", time.time() - t0)
        st.write("Number of clusters", len(clusters_df))
        st.write("Number of authors", clusters_df.num_authors.sum())
