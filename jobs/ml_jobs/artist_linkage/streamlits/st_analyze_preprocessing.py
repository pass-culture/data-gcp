# %%

import re

import pandas as pd
import streamlit as st

from match_artists_on_wikidata import preprocess_artists
from utils.preprocessing_utils import (
    clean_names,
    extract_first_artist,
    format_names,
)

st.set_page_config(
    page_title="Scratch Analyze Preprocessing",
    page_icon=":bar_chart:",
    layout="wide",
    initial_sidebar_state="expanded",
)
# Params
ARTIST_NAME_TO_FILTER = {
    "multi-artistes",
    "xxx",
    "compilation",
    "tbc",
    "divers",
    "a preciser",
    "aa.vv.",
    "etai",
    "france",
    "0",
    "wallpaper",
    "<<<<<",
    "nc",
}

# Columns
ARTIST_ID_KEY = "artist_id"
ID_KEY = "id"
PRODUCT_ID_KEY = "offer_product_id"
ARTIST_NAME_KEY = "artist_name"
ARTIST_NAME_TO_MATCH_KEY = "artist_name_to_match"
ARTIST_TYPE_KEY = "artist_type"
OFFER_CATEGORY_ID_KEY = "offer_category_id"
ID_PER_CATEGORY = "id_per_category"

NOT_MATCHED_WITH_ARTISTS_KEY = "not_matched_with_artists"
REMOVED_PRODUCTS_KEY = "removed_products"
MATCHED_WITH_ARTISTS_KEY = "matched_with_artists"
MERGE_COLUMNS = [PRODUCT_ID_KEY, ARTIST_TYPE_KEY]


def preprocess_before_matching(df: pd.DataFrame) -> pd.DataFrame:
    return (
        (df.pipe(clean_names).pipe(extract_first_artist).pipe(format_names))
        .pipe(preprocess_artists)
        .rename(columns={"alias": ARTIST_NAME_TO_MATCH_KEY})
        .assign(artist_name_to_match=lambda df: df.artist_name_to_match.str.strip())
        .loc[lambda df: ~df.artist_name_to_match.isin(ARTIST_NAME_TO_FILTER)]
        .filter(
            [
                PRODUCT_ID_KEY,
                ARTIST_TYPE_KEY,
                OFFER_CATEGORY_ID_KEY,
                ARTIST_NAME_KEY,
                ARTIST_NAME_TO_MATCH_KEY,
                ARTIST_ID_KEY,
                "true_artist_name",
            ],
        )
    )


def extract_true_artist_name(artist_name: str) -> str:
    """
    Extracts a clean, standardized artist name from a raw string.

    This function processes the raw artist name through a series of rules
    to handle multiple artists, different name formats (Last, First vs. First Last),
    initials, and other common variations.

    Args:
        artist_name: The raw artist name string.

    Returns:
        The cleaned artist name.
    """
    if not isinstance(artist_name, str) or not artist_name.strip():
        return ""

    # --- Step 1: Initial Cleaning and Pre-processing ---
    # Remove leading/trailing whitespace and surrounding quotes
    name = artist_name.strip().strip('"')

    # --- Step 2: Handle Hardcoded Edge Cases ---
    # These are specific cases that don't fit the general rules.
    if name.lower().startswith("collectif/"):
        name = name.lower().replace("collectif/", "").strip()

    # --- Step 3: Split Multiple Artists ---
    # Artists can be separated by various delimiters. We take the first one.
    name = re.split(r"\s*[/;&+]\s*", name)[0].strip()

    # --- Step 4: Handle "Lastname, Firstname" Format ---
    # This is a common format, e.g., "Gregson, Edward"
    if "," in name:
        # Can mean multiple artists or a single artist in "Lastname, Firstname" format.
        if len(name.split(",")) > 2:
            # If there are more than two parts, we assume it's multiple artists.
            # We take the first part as the main artist.
            name = name.split(",")[0].strip()
        elif len(name.split()) >= 4:
            # If the name has more than four parts, we assume it's two artists
            # and take the first part as the main artist.
            name = name.split(",")[0].strip()
        else:
            # Otherwise, we assume it's a single artist in "Lastname, Firstname" format.
            # We split by comma and then reformat to "Firstname Lastname".
            parts = name.split(",", 1)
            last_name = parts[0].strip()
            first_name = parts[1].strip()
            # Recombine as "Firstname Lastname"
            name = f"{first_name} {last_name}".strip()

    # --- Step 5: Handle "Lastname Initial(s)" Format ---
    # This is the most complex part. We need to distinguish the name from the initials.
    words = [word for word in re.split(r"\s+|-|\.", name) if word != ""]
    if len(words) < 2:
        # If it's a single word, there's nothing to reorder.
        return name

    # We iterate backwards to separate initials from the main name.
    # An "initial" is a short word that is not a particle.
    initial_parts = []
    lastname_parts = []

    for word in words:
        # A word is considered an initial if it's 1 alphabet character long
        is_initial = len(word.replace(".", "").replace("-", "")) <= 1 and word.isalpha()

        if is_initial:
            initial_parts.append(word)
        else:
            lastname_parts.append(word)

    # --- Step 6: Format and Recombine ---
    if initial_parts:
        # We have found and separated initials that need to be moved to the front.

        # Format the initials consistently (e.g., "p", "j" -> "p.j.")
        # Flatten all initial parts into a single string without separators.
        flat_initials = "".join(initial_parts).replace(".", "").replace("-", "")
        formatted_initials = ". ".join(list(flat_initials))
        if formatted_initials:
            # Add a trailing dot for consistency, e.g., "j.l."
            if not formatted_initials.endswith("."):
                formatted_initials += "."

        # Reconstruct the last name
        reordered_lastname = " ".join(lastname_parts)

        return f"{formatted_initials} {reordered_lastname}".strip()

    else:
        # No initials were found to reorder, but we might need to fix particles.
        reordered_name = " ".join(lastname_parts)

        # --- Step 7: Final Spacing and Formatting Fixes ---
        # Fix cases like "b.b.king" -> "b.b. king"
        # Use a lookbehind to add a space after a dot if it's followed by a letter.
        reordered_name = re.sub(r"(?<=\.)([a-zA-Z])", r" \1", reordered_name)

        # Normalize multiple spaces
        reordered_name = re.sub(r"\s+", " ", reordered_name)

        return reordered_name


test_set_df = pd.read_csv(
    "/home/laurent_pass/Projects/data-gcp/jobs/ml_jobs/artist_linkage/tests/utils/data/preprocessing_test_set.csv"
)

prediction_df = (
    test_set_df.assign(old_artist_name=lambda df: df.artist_name)
    .pipe(preprocess_before_matching)
    .drop_duplicates()
).assign(
    gemini_artist_name_to_match=lambda df: df.artist_name.apply(
        extract_true_artist_name
    )
)


with st.expander("Show preprocessed data", expanded=False):
    st.dataframe(prediction_df)

# Results
errors_df = prediction_df.loc[lambda df: df.true_artist_name != df.artist_name_to_match]
gemini_errors_df = prediction_df.loc[
    lambda df: df.true_artist_name != df.gemini_artist_name_to_match
]
cols = st.columns([1, 1])
cols[0].markdown("### Old Script")
with cols[0].expander("Show OK", expanded=False):
    st.dataframe(
        prediction_df.loc[lambda df: df.true_artist_name == df.artist_name_to_match]
    )
cols[0].dataframe(errors_df)
cols[0].write(
    (prediction_df.true_artist_name == prediction_df.artist_name_to_match).value_counts(
        normalize=True
    )
)
cols[1].markdown("### Gemini Script")
with cols[1].expander("Show OK", expanded=False):
    st.dataframe(
        prediction_df.loc[
            lambda df: df.true_artist_name == df.gemini_artist_name_to_match
        ]
    )
cols[1].dataframe(gemini_errors_df)
cols[1].write(
    (
        prediction_df.true_artist_name == prediction_df.gemini_artist_name_to_match
    ).value_counts(normalize=True)
)
