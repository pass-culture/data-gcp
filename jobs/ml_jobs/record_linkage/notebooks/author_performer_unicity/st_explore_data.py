import time
from collections import defaultdict

import pandas as pd
import rapidfuzz
import stqdm
import streamlit as st
from sklearn.cluster import AgglomerativeClustering

st.set_page_config(layout="wide")

# %% Load Data
author_df = pd.read_csv("notebooks/author_performer_unicity/data/author.csv")
CATEGORIES = author_df.offer_category_id.unique()
PUNCTUATION = r"!|#|\$|\%|\&|\(|\)|\*|\+|\,|\/|\:|\;|\|\s-|\s-\s|-\s|\|"  # '<=>?@[\\]^_`{|}~|\s–\s'


# %%
def preprocessing(string: str) -> str:
    return " ".join(sorted(rapidfuzz.utils.default_process(string).split()))


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

if selected_category == "LIVRE":
    filtered_df = (
        filtered_df.assign(
            author=lambda df: df.author.map(preprocessing)
            .str.replace(r"(\b[a-zA-Z]\b)", "", regex=True)
            .str.replace(r"\s+", " ", regex=True)
            .str.strip()
        )
        .loc[lambda df: (df.author.apply(lambda x: len(x.split())) > 1)]
        .drop_duplicates()
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


def normalized_distance_token_sort_ratio(
    string_a: str, string_b: str, **kwargs
) -> float:
    return 1 - rapidfuzz.fuzz.token_sort_ratio(string_a, string_b, **kwargs) / 100


def group_artist_names(names, threshold, method):
    grouped_names = defaultdict(list)
    seen_names = set()

    for name in stqdm.stqdm(names):
        # Check if the name is already seen or grouped
        if name.lower() not in seen_names:
            for grouped_name in grouped_names:

                if method(name, grouped_name) < threshold:
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

    st_method = st.selectbox(
        "method",
        options=[
            normalized_distance_token_sort_ratio,
            rapidfuzz.distance.DamerauLevenshtein.normalized_distance,
            rapidfuzz.distance.JaroWinkler.normalized_distance,
            rapidfuzz.distance.LCSseq.normalized_distance,
            rapidfuzz.distance.OSA.normalized_distance,
        ],
        format_func=lambda x: (
            x.func_code.co_name if hasattr(x, "func_code") else x.__name__
        ),
        index=4,
    )

    st_clustering_threshold = st.slider(
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

        # Advanced clustering
        # Compute the parwise distance between the authors
        t0 = time.time()
        N_WORKERS = -1  # -1 for max number of workers
        distance_matrix = rapidfuzz.process.cdist(
            queries=authors_list,
            choices=authors_list,
            scorer=st_method,
            workers=N_WORKERS,
        )
        st.write("Time to compute the matrix", time.time() - t0)

        # Perform hierarchical/agglomerative clustering
        t0 = time.time()

        clustering = AgglomerativeClustering(
            n_clusters=None,
            metric="precomputed",
            linkage="single",
            distance_threshold=st_clustering_threshold,
        )
        clusters = clustering.fit_predict(distance_matrix)

        clusters_df = (
            pd.DataFrame({"author": authors_list})
            .assign(cluster=clusters)
            .groupby("cluster")
            .agg({"author": set})
            .assign(num_authors=lambda df: df.author.map(len))
            .sort_values("num_authors", ascending=False)
        )
        st.write("Time to compute the clusters", time.time() - t0)

        # Display
        st.markdown("""---""")
        st.write("Number of clusters", len(clusters_df))
        st.write(
            "Number of clusters with at least 2 names",
            clusters_df.num_authors.gt(1).sum(),
        )
        st.write("Number of authors", clusters_df.num_authors.sum())
        st.dataframe(
            clusters_df,
            width=1500,
        )
