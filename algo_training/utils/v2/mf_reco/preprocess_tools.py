import pandas as pd
from pandas.api.types import CategoricalDtype
import numpy as np
import scipy.sparse as sparse


def weighted_interactions(interaction):
    interaction["eventStrength"] = (
        interaction["event_type"].apply(lambda x: event_type_strength[x])
        * interaction["event_count"]
    )
    return interaction


def weighted_fav(favorites):
    favorites = favorites[~favorites["offer_id"].isin(["NaN"])]
    favorites["eventStrength"] = (
        favorites["event_type"].apply(lambda x: event_type_strength[x]) * 1
    )
    return favorites


def group_data(bookings, clicks, favorites):
    bookings_clicks = bookings.append(clicks)

    bookings_clikcs_favorites = bookings_clicks.append(favorites)
    grouped_df = (
        bookings_clikcs_favorites.groupby(
            [
                "user_id",
                "offer_id",
                "offer_name",
                "offer_subcategoryid",
                "event_type",
                "user_age",
            ]
        )["eventStrength"]
        .sum()
        .reset_index()
    )
    return grouped_df


def clean_data(grouped_df, minimal_user_strength, minimal_offer_strength):
    grouped_df_offer_sum = grouped_df.groupby(["offer_id"], as_index=False)[
        "eventStrength"
    ].sum()

    grouped_df_user_sum = grouped_df.groupby(["user_id"], as_index=False)[
        "eventStrength"
    ].sum()

    cleaned_df_user_relevant = grouped_df_user_sum.query(
        f"eventStrength >{minimal_user_strength}"
    )
    list_user_to_keep = cleaned_df_user_relevant.user_id.tolist()

    cleaned_df_offer_relevant = grouped_df_offer_sum.query(
        f"eventStrength >{minimal_offer_strength}"
    )
    list_offer_to_keep = cleaned_df_offer_relevant.offer_id.tolist()

    df_cleaned = grouped_df[grouped_df.user_id.isin(list_user_to_keep)]

    df_cleaned = df_cleaned[df_cleaned.offer_id.isin(list_offer_to_keep)]
    return df_cleaned


def get_meta_and_sparse(grouped_purchased):
    customers = list(
        np.sort(grouped_purchased.user_id.unique())
    )  # Get our unique customers
    products = list(
        grouped_purchased.offer_id.unique()
    )  # Get our unique products that were purchased
    quantity = list(grouped_purchased.eventStrength)  # All of our purchases
    cat_type_row = CategoricalDtype(categories=customers, ordered=True)
    cat_type_col = CategoricalDtype(categories=products, ordered=True)
    rows = grouped_purchased.user_id.astype(cat_type_row).cat.codes
    # Get the associated row indices
    cols = grouped_purchased.offer_id.astype(cat_type_col).cat.codes
    # Get the associated column indices
    purchases_sparse = sparse.csr_matrix(
        (quantity, (rows, cols)), shape=(len(customers), len(products))
    )
    return purchases_sparse, np.array(customers), np.array(products)


def data_prep(df_cleaned):
    df_cleaned["user_id"] = df_cleaned.user_id.astype(
        int
    )  # Convert to int for customer ID
    df_cleaned = df_cleaned[
        [
            "offer_id",
            "eventStrength",
            "user_id",
            "offer_name",
            "offer_subcategoryid",
            "event_type",
        ]
    ]  # Get rid of unnecessary info
    grouped_cleaned = (
        df_cleaned.groupby(["user_id", "offer_id", "offer_name", "offer_subcategoryid"])
        .sum()
        .reset_index()
    )  # Group together
    grouped_cleaned.eventStrength.loc[
        grouped_cleaned.eventStrength == 0
    ] = 1  # Replace a sum of zero purchases with a one to
    # indicate purchased
    grouped_purchased = grouped_cleaned.query(
        "eventStrength > 0"
    )  # Only get customers where purchase totals were positive
    return grouped_purchased


def get_sparcity(purchases_sparse):
    matrix_size = (
        purchases_sparse.shape[0] * purchases_sparse.shape[1]
    )  # Number of possible interactions in the matrix
    num_purchases = len(
        purchases_sparse.nonzero()[0]
    )  # Number of items interacted with
    sparsity = 100 * (1 - (num_purchases / matrix_size))
    return sparsity


def get_feedback_sparcity_error(filters):
    minimal_user_strength = filters[0]
    minimal_offer_strength = filters[1]
    tol = 0.1
    df_cleaned = clean_data(grouped_df, minimal_user_strength, minimal_offer_strength)
    grouped_purchased = data_prep(df_cleaned)
    purchases_sparse, customers_arr, products_arr = get_meta_and_sparse(
        grouped_purchased
    )
    sparcity = get_sparcity(purchases_sparse)
    error = 99.5 - sparcity
    return error ** 2
