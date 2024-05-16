import streamlit as st

import pandas as pd
from collections import defaultdict

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
    return " ".join(sorted(string.lower().split(" ")))


def ratio(string_a: str, string_b: str, method: str) -> float:

    if method == "ratio":
        return rapidfuzz.fuzz.token_sort_ratio(string_a, string_b)
    if method == "damerau-levenshtein":
        return 100 * rapidfuzz.distance.DamerauLevenshtein.normalized_similarity(
            string_a, string_b
        )
    if method == "jaro-winkler":
        return 100 * rapidfuzz.distance.JaroWinkler.normalized_similarity(
            string_a, string_b
        )
    if method == "lcs-seq":
        return 100 * rapidfuzz.distance.LCSseq.normalized_similarity(string_a, string_b)


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
        "Levenstein threshold", min_value=0, max_value=100, step=1, value=90
    )
    st_method = st.selectbox(
        "method",
        options=["ratio", "jaro-winkler", "damerau-levenshtein", "lcs-seq"],
        index=3,
    )

    artist_names = filtered_df.author.map(preprocessing).tolist()
    grouped_names = group_artist_names(artist_names, st_threshold, st_method)
    matched_authors = (
        pd.DataFrame({"author": grouped_names})
        .assign(num_authors=lambda df: df.author.apply(lambda l: len(l)))
        .sort_values("num_authors", ascending=False)
    )
    st.dataframe(matched_authors, width=1500)
    # Every form must have a submit button.
    submitted = st.form_submit_button("Compute")
