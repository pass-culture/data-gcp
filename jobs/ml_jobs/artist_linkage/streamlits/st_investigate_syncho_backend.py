import pandas as pd
import streamlit as st

from src.constants import ARTIST_NAME_TO_FILTER
from src.utils.preprocessing_utils import (
    clean_names,
    extract_first_artist,
    format_names,
)

DEFAULT_EXPANDER_STATE = False
MERGE_COLUMNS = ["product_id", "artist_type"]
st.set_page_config(layout="wide")


def preprocess_artists(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        alias=lambda _df: _df.preprocessed_artist_name,
        tmp_id=lambda df: df.index,
    )


@st.cache_data
def load_artist_data() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    artist_df = pd.read_parquet(
        "streamlits/data/artist_linkage_stg_artist_table.parquet"
    )
    alias_df = pd.read_parquet(
        "streamlits/data/artist_linkage_stg_artist_alias_table.parquet"
    )
    product_artist_link_df = pd.read_parquet(
        "streamlits/data/artist_linkage_stg_product_artist_link_table.parquet"
    ).astype({"product_id": int})

    # TODO: remove that in the tables
    artist_df = artist_df.dropna(subset=["id"])
    alias_df = alias_df.dropna(subset=["artist_id"])
    product_artist_link_df = product_artist_link_df.dropna(subset=["artist_id"])
    return artist_df, alias_df, product_artist_link_df


@st.cache_data
def load_products() -> pd.DataFrame:
    return (
        pd.read_parquet(
            "/home/laurent_pass/Téléchargements/artist_linkage_stg_tmp_artist_product.parquet"
        )
        .rename(columns={"offer_product_id": "product_id"})
        .astype({"product_id": int})
        .dropna(subset=["artist_name"])
        .loc[lambda df: df.artist_name != ""]
        .drop_duplicates(
            subset=["product_id", "artist_type"]
        )  # To remove offers with the same product_id and artist_type
    )


@st.cache_data
def remove_data(product_artist_link_df):
    return product_artist_link_df.sample(frac=0.95)


def count_matched_products(
    products_df: pd.DataFrame, product_artist_link_df: pd.DataFrame
) -> pd.DataFrame:
    actual_product_ids = products_df.loc[:, MERGE_COLUMNS].reset_index(drop=True)
    linked_product_ids = product_artist_link_df.loc[:, MERGE_COLUMNS]
    return actual_product_ids.merge(
        linked_product_ids,
        how="outer",
        left_on=MERGE_COLUMNS,
        right_on=MERGE_COLUMNS,
        indicator=True,
    ).replace(
        {
            "_merge": {
                "left_only": "Not matched with artists",
                "right_only": "Removed products",
                "both": "Matched with artists",
            }
        }
    )


def preprocess_before_matching(df: pd.DataFrame) -> pd.DataFrame:
    return (
        (df.pipe(clean_names).pipe(extract_first_artist).pipe(format_names))
        .pipe(preprocess_artists)
        .rename(columns={"alias": "artist_name_to_match"})
        .filter(
            [
                "product_id",
                "artist_type",
                "offer_category_id",
                "artist_name",
                "artist_name_to_match",
                "artist_id",
            ],
        )
        .assign(artist_name_to_match=lambda df: df.artist_name_to_match.str.strip())
        .loc[lambda df: ~df.artist_name_to_match.isin(ARTIST_NAME_TO_FILTER)]
    )


def get_index_max_per_category_and_type(alias_df: pd.DataFrame) -> dict:
    return (
        alias_df.loc[lambda df: ~df.artist_id.str.startswith("Q")]
        .drop_duplicates("artist_id")
        .assign(
            id_per_category=lambda df: df.artist_id.str.split("_").str[-1].astype(int),
        )
        .groupby(["offer_category_id", "artist_type"])
        .agg({"id_per_category": lambda s: s.max() + 1})
        .to_dict()
    )["id_per_category"]


#################################################################################

# %% Load DATA
st.subheader("Load Data")
artist_df, alias_df, raw_product_artist_link_df = load_artist_data()
raw_products_df = load_products()


# %% Show Dataframes
with st.expander(f"Artist Table ({len(artist_df)})", expanded=DEFAULT_EXPANDER_STATE):
    st.dataframe(artist_df)

with st.expander(
    f"Artist Alias Table ({len(alias_df)})", expanded=DEFAULT_EXPANDER_STATE
):
    st.dataframe(alias_df)

with st.expander(
    f"Product Artist Link Table ({len(raw_product_artist_link_df)})",
    expanded=DEFAULT_EXPANDER_STATE,
):
    st.dataframe(raw_product_artist_link_df.sort_values(by=["product_id"]))

with st.expander(
    f"Product Table ({len(raw_products_df)})", expanded=DEFAULT_EXPANDER_STATE
):
    st.dataframe(raw_products_df.sort_values(by=["product_id"]))
st.divider()

# %% Check previous matching
st.subheader("Check matching")
with st.expander("Check matching", expanded=DEFAULT_EXPANDER_STATE):
    merged_df = count_matched_products(raw_products_df, raw_product_artist_link_df)
    st.markdown("### Before removing data")
    st.dataframe(merged_df)
    st.write(merged_df._merge.value_counts().reset_index())
    st.divider()

    # %% Remove data to simulate real life
    products_df = raw_products_df.pipe(remove_data)
    product_artist_link_df = raw_product_artist_link_df.pipe(remove_data)
    st.markdown("### After removing data")
    st.dataframe(products_df)
    st.dataframe(product_artist_link_df)

    merged_df = count_matched_products(products_df, product_artist_link_df)
    st.dataframe(merged_df)
    st.write(merged_df._merge.value_counts().reset_index())

#################################################################################

# %% Split products between to remove and to link
st.subheader("Split the Dataframe into the different options")
merged_df = count_matched_products(products_df, product_artist_link_df)

products_to_remove_df = merged_df.loc[
    lambda df: df._merge == "Removed products",
    MERGE_COLUMNS,
]
products_to_link_ids_df = merged_df.loc[
    lambda df: df._merge == "Not matched with artists",
    MERGE_COLUMNS,
]
products_to_link_df = products_to_link_ids_df.merge(
    products_df, how="left", on=MERGE_COLUMNS
)
with st.expander(
    f"Products to remove ({len(products_to_remove_df)})",
    expanded=DEFAULT_EXPANDER_STATE,
):
    st.write(products_to_remove_df)
with st.expander(
    f"Products to link ({len(products_to_link_df)})", expanded=DEFAULT_EXPANDER_STATE
):
    st.write(products_to_link_df)
st.divider()

# %% Preprocess names to match
st.subheader("Preprocess names")

### Params
preproc_products_to_link_df = preprocess_before_matching(products_to_link_df)
preproc_artist_alias_df = (
    preprocess_before_matching(
        alias_df.rename(columns={"artist_alias_name": "artist_name"})
    )
    .drop(columns=["artist_name"])
    .drop_duplicates()
)
with st.expander(
    f"Preprocessed products to link ({len(preproc_products_to_link_df)}), after filtering",
    expanded=DEFAULT_EXPANDER_STATE,
):
    st.write(preproc_products_to_link_df)
with st.expander(
    f"Preprocessed aliases ({len(preproc_artist_alias_df)})",
    expanded=DEFAULT_EXPANDER_STATE,
):
    st.write(preproc_artist_alias_df)

# %% Match names
st.subheader("Match names")
matched_df = preproc_products_to_link_df.merge(
    preproc_artist_alias_df,
    how="left",
    on=["artist_name_to_match", "artist_type", "offer_category_id"],
)
linked_products_df = matched_df.loc[lambda df: df.artist_id.notna()]
unlinked_products_df = matched_df.loc[lambda df: df.artist_id.isna()]

with st.expander(f"Matched names ({len(matched_df)})", expanded=DEFAULT_EXPANDER_STATE):
    st.write(matched_df)
with st.expander(
    f"Linked products ({len(linked_products_df)})", expanded=DEFAULT_EXPANDER_STATE
):
    st.write(linked_products_df)
with st.expander(
    f"Products to create artists ({len(unlinked_products_df)})",
    expanded=DEFAULT_EXPANDER_STATE,
):
    st.write(unlinked_products_df.sort_values(by=["artist_name_to_match"]))


count_df = (
    unlinked_products_df.groupby(
        ["artist_name_to_match", "artist_type"], as_index=False
    )
    .agg({"product_id": "count", "artist_name_to_match": "first"})
    .rename(columns={"product_id": "number_of_products"})
)
with st.expander(
    "Number of artists to create",
    expanded=DEFAULT_EXPANDER_STATE,
):
    cols = st.columns(2)
    with cols[0]:
        st.write(count_df.sort_values(by=["number_of_products"], ascending=False))
    with cols[1]:
        st.write(count_df.number_of_products.value_counts())

# %% Create artists for unlinked products
# st.subheader("Create artists, alias and artist_product_links for unlinked products")
st.subheader("Create artists for unlinked products")

# name clusters
index_max_per_category_and_type = get_index_max_per_category_and_type(alias_df)
new_artists_id_list = []
for group_name, group in unlinked_products_df.drop_duplicates(
    ["offer_category_id", "artist_type", "artist_name_to_match"]
).groupby(["offer_category_id", "artist_type"], as_index=False):
    offer_category_id = group_name[0]
    artist_type = group_name[1]
    new_artists_id_list.append(
        group.reset_index(drop=True).assign(
            group_index=lambda df: df.index,
            id=lambda df: offer_category_id
            + "_"
            + artist_type
            + "_"
            + (
                df.group_index
                + index_max_per_category_and_type.get(
                    (offer_category_id, artist_type), 0
                )
            ).astype(str),
        )
    )
new_artist_df = (
    pd.concat(new_artists_id_list)
    .loc[
        :,
        [
            "id",
            "offer_category_id",
            "artist_type",
            "artist_name",
            "artist_name_to_match",
        ],
    ]
    .reset_index(drop=True)
).rename(
    columns={
        "artist_name": "name",
        "artist_name_to_match": "name_to_match",
        "artist_type": "type",
    }
)
with st.expander(
    f"New artists ({len(new_artist_df)})", expanded=DEFAULT_EXPANDER_STATE
):
    st.write(new_artist_df)


# %% Create artist alias
st.subheader("Create artist alias for new artists")

new_artist_alias_df = (
    unlinked_products_df.merge(
        new_artist_df,
        how="left",
        left_on=["artist_name_to_match", "artist_type", "offer_category_id"],
        right_on=["name_to_match", "type", "offer_category_id"],
    )
    .loc[
        lambda df: df.artist_id.isna(),
        [
            "id",
            "offer_category_id",
            "artist_type",
            "artist_name",
        ],
    ]
    .drop_duplicates()
)
with st.expander(
    f"New artist alias ({len(new_artist_alias_df)})", expanded=DEFAULT_EXPANDER_STATE
):
    st.write(new_artist_alias_df)
# %% Create artist product links
st.subheader("Create artist product links for new artists")
new_artist_product_link_df = (
    unlinked_products_df.drop(columns=["artist_id"])
    .merge(
        new_artist_df.rename(columns={"id": "artist_id"}),
        how="left",
        left_on=["artist_name_to_match", "artist_type", "offer_category_id"],
        right_on=["name_to_match", "type", "offer_category_id"],
    )
    .loc[lambda df: df.artist_id.notna()]
)
with st.expander(
    f"New artist product links ({len(new_artist_product_link_df)})",
    expanded=DEFAULT_EXPANDER_STATE,
):
    st.write(new_artist_product_link_df)

# %% Create delta artist product links
st.subheader("Create delta artist product links")
PRODUCT_LINK_COLUMNS = ["product_id", "artist_id", "artist_type"]
delta_products_df = pd.concat(
    [
        linked_products_df.filter(PRODUCT_LINK_COLUMNS).assign(
            action="add", comment="linked artist"
        ),
        new_artist_product_link_df.filter(PRODUCT_LINK_COLUMNS).assign(
            action="add", comment="new artist"
        ),
        products_to_remove_df.filter(PRODUCT_LINK_COLUMNS).assign(
            action="remove", comment="removed product"
        ),
    ]
)
with st.expander(
    f"Delta artist product links ({len(delta_products_df)})",
    expanded=DEFAULT_EXPANDER_STATE,
):
    st.write(delta_products_df)
