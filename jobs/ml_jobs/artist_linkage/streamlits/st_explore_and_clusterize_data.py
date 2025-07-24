import time
from typing import Callable, List

import jellyfish
import numpy as np
import pandas as pd
import rapidfuzz
import streamlit as st
from scipy.sparse import csr_matrix, vstack
from sklearn.cluster import DBSCAN
from stqdm import stqdm

from constants import OFFER_IS_SYNCHRONISED
from utils.preprocessing_utils import (
    FilteringParamsType,
    clean_names,
    extract_first_artist,
    filter_artists,
    format_names,
)

st.set_page_config(layout="wide")

FILTERING_PARAMS = FilteringParamsType(
    min_word_count=2, max_word_count=5, min_offer_count=100, min_booking_count=100
)


# %% Load Data
@st.cache_data
def load_data(source_path: str) -> pd.DataFrame:
    return pd.read_parquet(source_path)


artist_df = pd.load_data(
    "gs://data-bucket-stg/link_artists/artists_to_match.parquet"
).assign(artist=lambda df: df.artist_name)

CATEGORIES = artist_df.offer_category_id.unique()
PUNCTUATION = r"!|#|\$|\%|\&|\(|\)|\*|\+|\,|\/|\:|\;|\|\s-|\s-\s|-\s|\|"  # '<=>?@[\\]^_`{|}~|\s–\s'
NUM_CHUNKS = 50
SPARSE_FILTER_THRESHOLD = 0.2
DTYPE_DISTANCE_MATRIX = np.uint8  # np.uint8, np.uint16, np.float32
SCORE_MULTIPLIER = (
    255
    if DTYPE_DISTANCE_MATRIX == np.uint8
    else 65535
    if DTYPE_DISTANCE_MATRIX == np.uint16
    else 1
)

selected_category = st.sidebar.selectbox(
    "category",
    options=CATEGORIES,
    index=0,  # len(CATEGORIES) - 1
)
only_synchronized = st.sidebar.checkbox("only synchronized", value=False)
only_booked = st.sidebar.checkbox("only booked", value=False)
selected_punctuation = st.sidebar.selectbox(
    "only with punctuation", options=["WITHOUT", "WITH", "NEVERMIND"]
)
search_filter = st.sidebar.text_input("search", value="oda")


# %%
category_df = artist_df.dropna().loc[
    lambda df: df.offer_category_id == selected_category
]
filtered_df = (
    category_df.loc[
        lambda df: df[OFFER_IS_SYNCHRONISED] if only_synchronized else df.index
    ]
    .loc[lambda df: df.total_booking_count > 0 if only_booked else df.index]
    .loc[
        lambda df: (
            df.artist_name.str.contains(PUNCTUATION)
            if selected_punctuation == "WITH"
            else (
                ~df.artist_name.str.contains(PUNCTUATION)
                if selected_punctuation == "WITHOUT"
                else df.index
            )
        )
    ]
    .loc[
        lambda df: (
            df.artist_name.str.contains(search_filter)
            if search_filter != ""
            else df.index
        )
    ]
)

preprocessed_df = (
    filtered_df.pipe(clean_names)
    .pipe(extract_first_artist)
    .pipe(filter_artists, filtering_params=FILTERING_PARAMS)
    .pipe(format_names)
)


# %% Print samples
col1, col2, col3 = st.columns(3)
with col1:
    st.write("Number of artists", len(category_df))
with col2:
    st.write("Number of artists after filtering", len(preprocessed_df))
with col3:
    st.progress(len(preprocessed_df) / len(category_df))

st.markdown("""---""")
if len(preprocessed_df) > 0:
    st.dataframe(
        preprocessed_df,
        width=1500,
        height=500,
        hide_index=True,
    )


# %% Clusterisation


def normalized_distance_token_sort_ratio(
    string_a: str, string_b: str, **kwargs
) -> float:
    return 1 - rapidfuzz.fuzz.token_sort_ratio(string_a, string_b, **kwargs) / 100


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def format_clustering_function(x):
    if hasattr(x, "func_code"):
        return x.func_code.co_name
    else:
        return x.__name__


@st.cache_data
def compute_distance_matrix(
    artists_list: List[str], _st_method: Callable, st_method_name: str
):
    # For caching with st_method_name
    # Compute the parwise distance between the artists
    t0 = time.time()

    # Loop over the chunks
    sparse_matrices = []
    for artists_chunk in stqdm(chunks(artists_list, len(artists_list) // NUM_CHUNKS)):
        # Compute the distance matrix for the chunk
        distance_matrix = rapidfuzz.process.cdist(
            queries=artists_chunk,
            choices=artists_list,
            scorer=_st_method,
            score_multiplier=SCORE_MULTIPLIER,
            dtype=DTYPE_DISTANCE_MATRIX,
            workers=-1,  # -1 for all cores
        )

        # Create the Sparse Matrix
        distance_matrix[
            distance_matrix > SPARSE_FILTER_THRESHOLD * SCORE_MULTIPLIER
        ] = 0
        sparse_matrices.append(csr_matrix(distance_matrix))

    # Concatenate the sparse matrices
    complete_sparse_matrix = vstack(blocks=sparse_matrices, format="csr")

    st.write("Time to compute the matrix", time.time() - t0)
    st.write("Memory used", complete_sparse_matrix.data.nbytes / 1024**2, "MB")

    return complete_sparse_matrix


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
        format_func=format_clustering_function,
        index=4,
    )

    st_clustering_threshold = st.slider(
        "Clustering threshold",
        min_value=0.0,
        max_value=1.0,
        step=0.01,
        value=0.2,
    )
    artists_list = preprocessed_df.preprocessed_artist_name.drop_duplicates().tolist()
    st.write("Number of artists", len(artists_list))

    ### Clustering
    submitted = st.form_submit_button("Compute")
    if submitted is True:
        complete_sparse_matrix = compute_distance_matrix(
            artists_list, st_method, format_clustering_function(st_method)
        )

        # Perform clustering with DBSCAN
        t0 = time.time()
        clustering = DBSCAN(
            eps=st_clustering_threshold * SCORE_MULTIPLIER,
            min_samples=2,
            metric="precomputed",
        )
        clustering.fit(complete_sparse_matrix)
        clusters = clustering.labels_

        clusters_df = (
            pd.DataFrame({"artist": artists_list})
            .assign(
                artist_encoding=lambda df: df.artist.map(jellyfish.metaphone),
                cluster=clusters,
            )
            .assign(
                cluster=lambda df: df.cluster.where(
                    lambda s: s != -1,
                    np.arange(df.cluster.max() + 1, df.cluster.max() + 1 + len(df)),
                )
            )
            .groupby("cluster")
            .agg({"artist": set, "artist_encoding": set})
            .assign(num_artists=lambda df: df.artist.map(len))
            .assign(num_encodings=lambda df: df.artist_encoding.map(len))
            .sort_values("num_artists", ascending=False)
        )
        st.write("Time to compute the clusters", time.time() - t0)

        # Display
        st.markdown("""---""")
        st.write("Number of clusters", len(clusters_df))
        st.write(
            "Number of clusters with at least 2 names",
            clusters_df.num_artists.gt(1).sum(),
        )
        st.write("Number of artists", clusters_df.num_artists.sum())
        st.dataframe(
            clusters_df,
            width=1500,
        )
