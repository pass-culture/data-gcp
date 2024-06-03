import string

import jellyfish
import numpy as np
import pandas as pd
import rapidfuzz
import streamlit as st

st.set_page_config(layout="wide")


# %% Load Data
@st.cache_data
def load_data(gs_path: str) -> pd.DataFrame:
    return pd.read_parquet(gs_path)


artist_df = load_data(
    "gs://data-bucket-stg/link_artists/artists_to_match.parquet"
).assign(artist=lambda df: df.artist_name)

CATEGORIES = artist_df.offer_category_id.unique()
PUNCTUATION = r"!|#|\$|\%|\&|\(|\)|\*|\+|\,|\/|\:|\;|\|\s-|\s-\s|-\s|\|"  # '<=>?@[\\]^_`{|}~|\s–\s'
NUM_CHUNKS = 10
SPARSE_FILTER_THRESHOLD = 0.2
DTYPE_DISTANCE_MATRIX = np.uint8  # np.uint8, np.uint16, np.float32
SCORE_MULTIPLIER = (
    255
    if DTYPE_DISTANCE_MATRIX == np.uint8
    else 65535
    if DTYPE_DISTANCE_MATRIX == np.uint16
    else 1
)


# %%
def preprocessing(string: str) -> str:
    return " ".join(sorted(rapidfuzz.utils.default_process(string).split()))


def should_be_filtered(artist_df: pd.DataFrame) -> bool:
    pattern = "[\w\-\.]+\/[\w-]+|\+"  # patter for multi artists separated by + or /

    # TODO: Remove artists with the pattern a. ....
    return artist_df.assign(
        should_be_filtered_pattern=lambda df: df.artist_name.str.contains(
            pattern, regex=True
        ),
        should_be_filtered_word_count=lambda df: (
            (df.artist_word_count <= 1)
            & ((df.offer_number < 100) | (df.total_booking_count < 100))
        )
        | (df.artist_word_count >= 6),
    )


def remove_leading_punctuation(artist_df: pd.DataFrame) -> pd.DataFrame:
    return artist_df.assign(
        artist_name=lambda df: df.artist_name.str.lstrip(
            string.whitespace + string.punctuation
        ).str.replace("\(.*\)", "", regex=True)
    )


def remove_parenthesis(artist_df: pd.DataFrame) -> pd.DataFrame:
    return artist_df.assign(
        artist_name=lambda df: df.artist_name.str.replace("\([.*]+\))", "")
        .str.split("\(", regex=True)
        .map(lambda ll: ll[0])
    )


# %%
def extract_artist_word_count(artist_df: pd.DataFrame) -> pd.DataFrame:
    return artist_df.assign(
        artist_word_count=lambda df: df.first_artist_comma.str.split().map(len)
    )


def extract_first_artist_pattern(artist_df: pd.DataFrame):
    pattern = ";|/|\+|\&"
    return artist_df.assign(
        first_artist_pattern=lambda df: df.artist_name.str.split(
            pattern, regex=True
        ).map(lambda artist_list: artist_list[0]),
        is_multi_artists_pattern=lambda df: df.artist_name.str.contains(
            pattern, regex=True
        ),
    )


def extract_first_artist_comma(artist_df: pd.DataFrame):
    pattern = "^(?![\w\-']+,).*,.*|.*,.*,.*"
    return artist_df.assign(
        is_multi_artists_comma=lambda df: (
            df.first_artist_pattern.str.contains(pattern, regex=True)
            & (~df.is_multi_artists_pattern)
        ),
        first_artist_comma=lambda df: df.first_artist_pattern.str.split(",", regex=True)
        .map(lambda artist_list: artist_list[0])
        .where(df.is_multi_artists_comma, df.first_artist_pattern),
    )


def find_muti_artists_comma(artist_name_series: pd.Series):
    return artist_name_series.str.contains("^(?![\w-]+,).*,.*", regex=True)


st_artist_type = st.sidebar.selectbox(
    "artist type", options=artist_df.artist_type.unique(), index=0
)
selected_category = st.sidebar.selectbox(
    "category",
    options=CATEGORIES,
    index=0,  # len(CATEGORIES) - 1
)
only_synchronized = st.sidebar.checkbox("only synchronized", value=True)
only_booked = st.sidebar.checkbox("only booked", value=False)
selected_punctuation = st.sidebar.selectbox(
    "only with punctuation", options=["WITHOUT", "WITH", "NEVERMIND"], index=2
)
search_filter = st.sidebar.text_input("search", value="oda")

category_df = (
    artist_df.dropna()
    .loc[lambda df: df.offer_category_id == selected_category]
    .loc[lambda df: df.artist_type == st_artist_type]
)
preprocessed_df = (
    category_df.loc[lambda df: df.is_synchronised if only_synchronized else df.index]
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
    .pipe(remove_leading_punctuation)
    .pipe(remove_parenthesis)
    .pipe(extract_first_artist_pattern)
    .pipe(extract_first_artist_comma)
    .pipe(extract_artist_word_count)
    .pipe(should_be_filtered)
    .sort_values(
        by=["artist_word_count", "total_booking_count", "offer_number"],
        ascending=[True, False, False],
    )
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
        preprocessed_df.loc[
            :,
            [
                "artist",
                "is_multi_artists_pattern",
                "first_artist_pattern",
                "is_multi_artists_comma",
                "first_artist_comma",
                "artist_word_count",
                "total_booking_count",
                "offer_number",
                "should_be_filtered_pattern",
                "should_be_filtered_word_count",
            ],
        ],
        width=1500,
        height=500,
        hide_index=True,
    )

st.write(
    pd.DataFrame(
        preprocessed_df.artist_word_count.value_counts()
        .sort_index()
        .rename("word_count")
    ).T
)
st.markdown("---")

## Filtering
filtered_df = (
    preprocessed_df.assign(
        preprocessed_name=lambda df: df.first_artist_comma.map(preprocessing),
        encoded_name=lambda df: df.preprocessed_name.map(jellyfish.metaphone),
    )
    .loc[lambda df: ~(df.should_be_filtered_pattern | df.should_be_filtered_word_count)]
    .loc[lambda df: df.is_synchronised]
    .sort_values(by="preprocessed_name")
)

st.dataframe(
    filtered_df.loc[
        :,
        [
            "artist",
            "first_artist_comma",
            "preprocessed_name",
            "encoded_name",
            "total_booking_count",
            "offer_number",
        ],
    ],
    width=1500,
    height=500,
    hide_index=True,
)
st.markdown("---")

## Pseudo Clustering
clusted_df = filtered_df.groupby("preprocessed_name").apply(
    lambda g: pd.DataFrame({"cluster_name": [g.name], "aliases": [set(g.artist)]})
)
st.write(clusted_df)

st.write(len(filtered_df.preprocessed_name.unique()))
