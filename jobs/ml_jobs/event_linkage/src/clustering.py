import hashlib
import uuid

import networkx as nx
import pandas as pd

from src.constants import (
    DESCRIPTION_SIMILARITY_COL,
    DESCRIPTION_SIMILARITY_THRESHOLD,
    EVENT_DESCRIPTION_COL,
    EVENT_IMAGE_URL_COL,
    EVENT_NAME_COL,
    FULL_DESCRIPTION_SIMILARITY_COL,
    FULL_NAME_SIMILARITY_COL,
    IMAGE_SIMILARITY_COL,
    IMAGE_SIMILARITY_THRESHOLD,
    NAME_SIMILARITY_COL,
    NAME_SIMILARITY_THRESHOLD,
    OFFER_ID_COL,
    OFFER_SUBCATEGORY_ID_COL,
    PARTIAL_NAME_SIMILARITY_THRESHOLD,
)
from src.interfaces import ClusterRepresentantMethod

DESCRIPTION_MATCH_COL = "description_match"
NAME_MATCH_COL = "name_match"
IMAGE_MATCH_COL = "image_match"
MATCH_COL = "match"


# Matching Params
SUBCATEGORIES_NOT_MATCHING_ON_OFFER_NAMES = ["SPECTACLE_REPRESENTATION"]


def get_uuid_from_cluster(offer_ids: set[str]) -> str:
    """
    Generate a UUID based on the offer IDs in the cluster.
        This ensures that the same cluster will always have the same UUID,
        even if the order of offers in the cluster changes.

    Args:
        offer_ids (set[str]): The set of offer IDs in the cluster.

    Returns:
        str: A UUID string generated from the offer IDs.
    """
    digest = hashlib.sha256(",".join(sorted(offer_ids)).encode()).hexdigest()
    return str(uuid.uuid5(uuid.NAMESPACE_URL, digest))


def should_match_on_offer_names(df: pd.DataFrame, subcategory_id: str) -> pd.Series:
    """
    For some subcategories, offer names are not discriminant and can be very similar
        even for different events (e.g. "Spectacle"). In those cases, we only consider
        description and image similarities to determine if two offers match.

    Args:
        df (pd.DataFrame): The dataframe containing the offers to compare.
        subcategory_id(str): The subcategory ID of the offers being compared.
    Returns:
        pd.Series: A boolean series indicating whether the offers match based on
            the selected criteria.
    """
    if subcategory_id in SUBCATEGORIES_NOT_MATCHING_ON_OFFER_NAMES:
        return df[DESCRIPTION_MATCH_COL] | df[IMAGE_MATCH_COL]

    return df[DESCRIPTION_MATCH_COL] | df[NAME_MATCH_COL] | df[IMAGE_MATCH_COL]


def compute_matched_pairs(cross_df: pd.DataFrame, subcategory_id: str) -> pd.DataFrame:
    """
    Filter offer pairs of a subcategory down to those that match on
        name, description, or image similarity (per subcategory rules).

    Args:
        cross_df (pd.DataFrame): The dataframe containing the offers and
            their similarities.
        subcategory_id (str): The subcategory ID to filter offers by.

    Returns:
        pd.DataFrame: The offer pairs of the subcategory that match, with
            all similarity columns and match flag columns.
    """
    # Filter offers by subcategory and similarity thresholds
    selected_df = (
        cross_df.loc[lambda df: df[f"{OFFER_SUBCATEGORY_ID_COL}_1"] == subcategory_id]
        .loc[
            lambda df: df["partial_name_similarity"]
            >= PARTIAL_NAME_SIMILARITY_THRESHOLD
        ]
        .reset_index(drop=True)
    ).assign(
        **{
            DESCRIPTION_MATCH_COL: lambda df: df[DESCRIPTION_SIMILARITY_COL]
            >= DESCRIPTION_SIMILARITY_THRESHOLD,
            NAME_MATCH_COL: lambda df: df[NAME_SIMILARITY_COL]
            >= NAME_SIMILARITY_THRESHOLD,
            IMAGE_MATCH_COL: lambda df: df[IMAGE_SIMILARITY_COL]
            >= IMAGE_SIMILARITY_THRESHOLD,
            MATCH_COL: lambda df: should_match_on_offer_names(df, subcategory_id),
        }
    )

    return selected_df[selected_df[MATCH_COL]]


def build_clusters_from_matched_pairs(
    matched_df: pd.DataFrame, subcategory_id: str
) -> pd.DataFrame:
    """
    Build clusters (connected components) of offers from a set of matched
        offer pairs.

    Args:
        matched_df (pd.DataFrame): Offer pairs that match, with columns
            "offer_id_1" and "offer_id_2".
        subcategory_id (str): The subcategory ID the pairs belong to, stored
            alongside each cluster.

    Returns:
        pd.DataFrame: A dataframe containing the clusters of offers.
    """
    G = nx.from_pandas_edgelist(
        matched_df, source=f"{OFFER_ID_COL}_1", target=f"{OFFER_ID_COL}_2"
    )
    return pd.DataFrame({"cluster": list(nx.connected_components(G))}).assign(
        cluster_length=lambda df: df.cluster.map(len), subcategory_id=subcategory_id
    )


def clusterize_offers(cross_df: pd.DataFrame, subcategory_id: str) -> pd.DataFrame:
    """
    Clusterize offers into events based on their similarities.

    Args:
        cross_df (pd.DataFrame): The dataframe containing the offers and
            their similarities.
        subcategory_id (str): The subcategory ID to filter offers by.

    Returns:
        pd.DataFrame: A dataframe containing the clusters of offers.
    """
    matched_df = compute_matched_pairs(cross_df, subcategory_id)
    return build_clusters_from_matched_pairs(matched_df, subcategory_id)


def get_cluster_metadata_representant(
    similarity_df: pd.DataFrame, metric_column: str, method: ClusterRepresentantMethod
) -> str | None:
    """
    Select a representative offer for a cluster based on the given method.

    Args:
        similarity_df (pd.DataFrame): The dataframe containing the offers in the cluster
            and expected to have at least an "offer_id" column and the metric column.
        metric_column (str): The column name to use as the similarity/connectivity
            metric.
        method (ClusterRepresentantMethod): The method to use for selecting the
            cluster representative:
            - MAX_SIMILARITY: selects the offer with the highest similarity metric value
            - MAX_EXACT_SIMILARITY: selects the offer with the most common similarity
                metric value, and among those, the one with the highest metric value.

    Returns:
        str | None: The offer ID of the selected cluster representative, or None if no
            offer has a non-zero metric value.

    Raises:
        ValueError: If an unknown method is provided.
    """
    filtered_df = similarity_df.loc[lambda df: df[metric_column] > 0.0]
    if len(filtered_df) == 0:
        return None

    # For MAX_SIMILARITY, we simply select the offer with the highest similarity
    if method == ClusterRepresentantMethod.MAX_SIMILARITY:
        return filtered_df.sort_values(metric_column, ascending=False).iloc[0][
            OFFER_ID_COL
        ]

    # For MAX_EXACT_SIMILARITY, we select offers that have the most common similarity
    #   , and among those we select the one with the highest similarity
    elif method == ClusterRepresentantMethod.MAX_EXACT_SIMILARITY:
        most_common_similarities = (
            filtered_df[metric_column].value_counts().loc[lambda s: s == s.max()]
        )
        biggest_similarity = most_common_similarities.index.max()
        return filtered_df.loc[lambda df: df[metric_column] == biggest_similarity].iloc[
            0
        ][OFFER_ID_COL]
    else:
        raise ValueError(f"Unknown method {method} for cluster representant selection")


def extract_cluster_metadata(
    cluster_row: pd.Series, cross_df: pd.DataFrame, raw_data_df: pd.DataFrame
) -> dict[str, str | None]:
    """
    Extract representative metadata (name, description, image) for a cluster of offers.

    For each metadata field, a representative offer is selected from the cluster using
    similarity scores: the offer with the highest name/description similarity is chosen
    for text fields, and the offer with the most common image similarity value is chosen
    for the image field.

    Args:
        cluster_row (pd.Series): A row from the clusters dataframe, expected to have a
            "cluster" field containing a set of offer IDs belonging to the cluster.
        cross_df (pd.DataFrame): The dataframe containing pairwise similarity scores
            between offers, with columns "offer_id_1", "offer_id_2", and the relevant
            similarity metric columns.
        raw_data_df (pd.DataFrame): The dataframe containing raw offer data, expected
            to have columns "offer_id", "offer_name", "offer_description", and
            "image_url".

    Returns:
        dict[str, str | None]: A dictionary with the following keys:
            - "event_name": the offer name of the most representative offer for the
                cluster name, or None if no valid similarity was found.
            - "event_description": the offer description of the most representative
                offer for the cluster description, or None if no valid similarity was
                found.
            - "event_image_url": the image URL of the most representative offer for
                the cluster image, or None if no valid similarity was found.
    """
    # Retrieve offers and similarities in cluster
    offers_in_cluster = raw_data_df[raw_data_df[OFFER_ID_COL].isin(cluster_row.cluster)]
    similarities_in_cluster = cross_df.loc[
        lambda df, o=offers_in_cluster: df[f"{OFFER_ID_COL}_1"].isin(o[OFFER_ID_COL])
        & df[f"{OFFER_ID_COL}_2"].isin(o[OFFER_ID_COL])
    ]

    # Get most common attributes in cluster
    concat_similarities = pd.concat(
        [
            similarities_in_cluster.rename(columns={f"{OFFER_ID_COL}_1": OFFER_ID_COL}),
            similarities_in_cluster.rename(columns={f"{OFFER_ID_COL}_2": OFFER_ID_COL}),
        ]
    )
    offer_name_representant = get_cluster_metadata_representant(
        concat_similarities,
        FULL_NAME_SIMILARITY_COL,
        ClusterRepresentantMethod.MAX_SIMILARITY,
    )
    offer_description_representant = get_cluster_metadata_representant(
        concat_similarities,
        FULL_DESCRIPTION_SIMILARITY_COL,
        ClusterRepresentantMethod.MAX_SIMILARITY,
    )
    offer_image_representant = get_cluster_metadata_representant(
        concat_similarities,
        IMAGE_SIMILARITY_COL,
        ClusterRepresentantMethod.MAX_EXACT_SIMILARITY,
    )

    return {
        EVENT_NAME_COL: raw_data_df.loc[
            lambda df: df[OFFER_ID_COL] == offer_name_representant,
            "offer_name",
        ].values[0]
        if offer_name_representant is not None
        else None,
        EVENT_DESCRIPTION_COL: raw_data_df.loc[
            lambda df: df[OFFER_ID_COL] == offer_description_representant,
            "offer_description",
        ].values[0]
        if offer_description_representant is not None
        else None,
        EVENT_IMAGE_URL_COL: raw_data_df.loc[
            lambda df: df[OFFER_ID_COL] == offer_image_representant,
            "image_url",
        ].values[0]
        if offer_image_representant is not None
        else None,
    }
