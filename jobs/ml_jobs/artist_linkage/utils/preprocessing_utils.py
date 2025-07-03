import re
import string
import unicodedata
from typing import TypedDict

import pandas as pd

from constants import TOTAL_OFFER_COUNT

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


### Cleaning
def _remove_leading_punctuation(artist_df: pd.DataFrame) -> pd.DataFrame:
    """
    Removes leading punctuation and parentheses from the artist names in the given DataFrame.

    Args:
        artist_df (pd.DataFrame): The DataFrame containing the artist names.
            Required columns: artist_name.

    Returns:
        pd.DataFrame: The DataFrame with leading punctuation and parentheses removed from the artist names.
    """
    return artist_df.assign(
        artist_name=lambda df: df.artist_name.str.lstrip(string.punctuation).str.rstrip(
            string.punctuation
        )
    )


def _remove_parenthesis(artist_df: pd.DataFrame) -> pd.DataFrame:
    """
    Removes parentheses and the contents inside from the artist names in the given DataFrame.

    Args:
        artist_df (pd.DataFrame): The DataFrame containing the artist names.
            Required columns: artist_name.

    Returns:
        pd.DataFrame: The DataFrame with parentheses removed from the artist names.
    """
    return artist_df.assign(
        artist_name=lambda df: df.artist_name.str.replace("\([.*]+\))", "")
        .str.split("\(", regex=True)
        .map(lambda ll: ll[0])
    )


def clean_names(artist_df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the names in the artist DataFrame.

    Args:
        artist_df (pd.DataFrame): The DataFrame containing artist names.
            Required columns: artist_name.

    Returns:
        pd.DataFrame: The cleaned DataFrame with artist names.

    """
    return (
        artist_df.pipe(_remove_leading_punctuation)
        .pipe(_remove_parenthesis)
        .assign(artist_name=lambda df: df.artist_name.str.strip())
    )


### Multi Artists


def _extract_first_artist_pattern(artist_df: pd.DataFrame):
    """
    Extracts the first artist thanks to a multi-artists punctuation pattern from the artist_name column of the given DataFrame.

    Args:
        artist_df (pd.DataFrame): The DataFrame containing the artist_name column.
            Required columns: artist_name.

    Returns:
        pd.DataFrame: The input DataFrame with two additional columns:
            - first_artist_pattern: The first artist pattern extracted from the artist_name column.
            - is_multi_artists_pattern: A boolean column indicating whether the artist_name contains multiple patterns.
    """
    pattern = ";|/|\+|\&"
    return artist_df.assign(
        first_artist_pattern=lambda df: df.artist_name.str.split(
            pattern, regex=True
        ).map(lambda artist_list: artist_list[0]),
        is_multi_artists_pattern=lambda df: df.artist_name.str.contains(
            pattern, regex=True
        ),
    )


def _extract_first_artist_comma(artist_df: pd.DataFrame):
    """
    Extracts the first artist from the given artist DataFrame by splitting the first_artist_pattern column on commas.
    If the first_artist_pattern column contains multiple artists, it selects the first artist.
    If the first_artist_pattern column does not contain multiple artists, it returns the value as is.

    Args:
        artist_df (pd.DataFrame): The DataFrame containing the artist information.
            Required columns: first_artist_pattern, is_multi_artists_pattern.

    Returns:
        pd.DataFrame: The DataFrame with additional columns 'is_multi_artists' and 'first_artist'.

    """
    pattern = "^(?![\w\-']+,).*,.*|.*,.*,.*"
    return artist_df.assign(
        is_multi_artists_comma=lambda df: (
            df.first_artist_pattern.str.contains(pattern, regex=True)
            & (~df.is_multi_artists_pattern)
        ),
        first_artist=lambda df: df.first_artist_pattern.str.split(",", regex=True)
        .map(lambda artist_list: artist_list[0])
        .where(df.is_multi_artists_comma, df.first_artist_pattern),
        is_multi_artists=lambda df: df.is_multi_artists_pattern
        | df.is_multi_artists_comma,
    )


def _remove_single_characters(artist_df: pd.DataFrame) -> pd.DataFrame:
    """
    Removes single characters from the 'first_artist' column of the input DataFrame.

    Args:
        artist_df (pd.DataFrame): The input DataFrame containing the 'first_artist' column.
            Required columns: first_artist.

    Returns:
        pd.DataFrame: The modified DataFrame with single characters removed from the 'first_artist' column.
    """
    pattern = r"\b[a-zA-Z]\b(?!\.)"
    return artist_df.assign(
        first_artist=lambda df: df.first_artist.str.strip()
        .str.replace(pattern, "", regex=True)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )


def extract_first_artist(artist_df: pd.DataFrame) -> pd.DataFrame:
    """
    Extracts the first artist from the given DataFrame.

    Args:
        artist_df (pd.DataFrame): The DataFrame containing artist information.
            Required columns: artist_name.

    Returns:
        pd.DataFrame: The DataFrame with the first artist extracted.
    """
    return (
        artist_df.pipe(_extract_first_artist_pattern)
        .pipe(_extract_first_artist_comma)
        .pipe(_remove_single_characters)
    )


### Filtering


def _extract_artist_word_count(artist_df: pd.DataFrame) -> pd.DataFrame:
    """
    Extracts the word count of each artist name in the given DataFrame. The word count is calculated by counting the number of words
        in the artist name that are longer than 2 characters (excluding punctuation / initials).

    Args:
        artist_df (pd.DataFrame): The DataFrame containing the artist names.
            Required columns: first_artist.

    Returns:
        pd.DataFrame: The input DataFrame with an additional column 'artist_word_count' that represents the word count of each artist name.
    """
    num_characters = 2

    # count the number of words in the artist name that are longer than 2 characters (excluding punctuation / initials)
    return artist_df.assign(
        artist_word_count=lambda df: df.first_artist.str.replace(
            r"[^\w\s]", "", regex=True
        )
        .str.split()
        .map(lambda ll: len([x for x in ll if len(x) > num_characters])),
    )


class FilteringParamsType(TypedDict):
    min_word_count: int
    max_word_count: int
    min_offer_count: int
    min_booking_count: int


def _filter_artists(
    artist_df: pd.DataFrame, filtering_params: FilteringParamsType
) -> bool:
    """
    Filters the artist DataFrame based on various criteria.

    The function filters out artists if:
    - The 'first_artist' name contains patterns like "word/word" or "word+word".
    - The 'artist_word_count' is below 'min_word_count' AND either 'total_offer_count' is below 'min_offer_count'
      OR 'total_booking_count' is below 'min_booking_count'.
    - The 'artist_word_count' is above 'max_word_count'.
    - The 'first_artist' name is empty or NaN.

    Args:
        artist_df (pd.DataFrame): The DataFrame to filter.
            Required columns: first_artist, artist_word_count, TOTAL_OFFER_COUNT, total_booking_count.
        filtering_params (FilteringParamsType): A dictionary containing filtering parameters:
            - min_word_count (int): Minimum word count for an artist name.
            - max_word_count (int): Maximum word count for an artist name.
            - min_offer_count (int): Minimum total offer count for an artist.
            - min_booking_count (int): Minimum total booking count for an artist.

    Returns:
        pd.DataFrame: The filtered DataFrame, containing only the rows that do not meet the filtering criteria.
    """
    pattern = "[\w\-\.]+\/[\w-]+|\+"  # pattern for multi artists separated by + or /

    matching_patterns_indexes = artist_df.first_artist.str.contains(pattern, regex=True)
    too_few_words_indexes = (
        artist_df.artist_word_count < filtering_params["min_word_count"]
    ) & (
        (artist_df[TOTAL_OFFER_COUNT] < filtering_params["min_offer_count"])
        | (artist_df.total_booking_count < filtering_params["min_booking_count"])
    )
    too_many_words_indexes = (
        artist_df.artist_word_count > filtering_params["max_word_count"]
    )
    empty_preprocessed_artist_names = (artist_df.preprocessed_artist_name == "") | (
        artist_df.preprocessed_artist_name.isna()
    )

    should_be_filtered = (
        matching_patterns_indexes
        | too_few_words_indexes
        | too_many_words_indexes
        | empty_preprocessed_artist_names
    )

    return artist_df.loc[~should_be_filtered]


def filter_artists(
    artist_df: pd.DataFrame, filtering_params: FilteringParamsType
) -> pd.DataFrame:
    """
    Filters the artist DataFrame based on the given filtering parameters.

    Args:
        artist_df (pd.DataFrame): The DataFrame containing artist data.
            Required columns: first_artist, artist_word_count, total_offer_count, total_booking_count.
        filtering_params (FilteringParamsType): The filtering parameters to be applied.

    Returns:
        pd.DataFrame: The filtered artist DataFrame.
    """
    return artist_df.pipe(_extract_artist_word_count).pipe(
        _filter_artists, filtering_params=filtering_params
    )


### Formatting


def format_names(artist_df: pd.DataFrame) -> pd.DataFrame:
    """
    Formats the artist names in the given DataFrame.

    Args:
        artist_df (pd.DataFrame): The DataFrame containing artist information.
            Required columns: first_artist.

    Returns:
        pd.DataFrame: The DataFrame with formatted artist names.
    """
    return artist_df.assign(
        preprocessed_first_artist=lambda df: normalize_string_series(
            df.first_artist.astype(str)
        ),
        part_1=lambda df: df.preprocessed_first_artist.str.split(",").str[0],
        part_2=lambda df: df.preprocessed_first_artist.str.split(",").str[1],
        preprocessed_artist_name=lambda df: df.part_1.where(
            df.part_2.isna(), df.part_2.astype(str) + " " + df.part_1.astype(str)
        ).str.strip(),
    ).drop(columns=["part_1", "part_2", "preprocessed_first_artist"])


def normalize_string_series(s: pd.Series) -> pd.Series:
    """
    Normalize a pandas Series of strings by converting to lowercase, removing accents,
    encoding to ASCII, stripping whitespace, and removing periods.
    Args:
        s (pd.Series): A pandas Series containing strings to be normalized.
    Returns:
        pd.Series: A pandas Series with normalized strings.
    """

    return (
        s.str.lower()
        .str.normalize("NFKD")
        .str.encode("ascii", errors="ignore")
        .str.decode("utf-8")
        .str.strip()
        .str.replace(".", "")
    )


def extract_artist_name(artist_name: str) -> str:
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

    def _remove_accents(text):
        return "".join(
            char
            for char in unicodedata.normalize("NFD", text)
            if unicodedata.category(char) != "Mn"
        )

    if (
        not isinstance(artist_name, str)
        or not artist_name.strip()
        or artist_name.strip() in ARTIST_NAME_TO_FILTER
    ):
        return None

    # --- Step 1: Initial Cleaning and Pre-processing ---
    # Remove accents, leading/trailing whitespace and surrounding quotes
    name = _remove_accents(artist_name.strip().strip('"').lower())

    # --- Step 2: Handle Hardcoded Edge Cases ---
    # These are specific cases that don't fit the general rules.
    if name.lower().startswith("collectif"):
        pattern = r"collectif\s*[^\w\s]*"
        name = re.sub(pattern, "", name.lower()).strip()

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
