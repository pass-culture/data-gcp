import pandas as pd
from pandas.api.types import CategoricalDtype
import numpy as np
import scipy.sparse as sparse
from scipy.optimize import minimize, Bounds

EVENT_TYPE_STRENGTH = {"CLICK": 1.0, "FAVORITE": 3.0, "BOOKING": 10.0}


def weighted_interactions(interaction):
    interaction["eventStrength"] = (
        interaction["event_type"].apply(lambda x: EVENT_TYPE_STRENGTH[x])
        * interaction["event_count"]
    )
    return interaction


def weighted_fav(favorites):
    favorites = favorites[~favorites["offer_id"].isin(["NaN"])]
    favorites["eventStrength"] = (
        favorites["event_type"].apply(lambda x: EVENT_TYPE_STRENGTH[x]) * 1
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


def get_feedback_sparcity_error(filters, dfgrp):
    minimal_user_strength = filters[0]
    minimal_offer_strength = filters[1]
    tol = 0.1
    df_cleaned = clean_data(dfgrp, minimal_user_strength, minimal_offer_strength)
    grouped_purchased = data_prep(df_cleaned)
    purchases_sparse, customers_arr, products_arr = get_meta_and_sparse(
        grouped_purchased
    )
    sparcity = get_sparcity(purchases_sparse)
    error = 99.5 - sparcity
    return error ** 2


def get_sparcity_filters(df):
    x1min = 5
    x1max = 50
    x2min = 100
    x2max = 1000
    bounds = Bounds([x1min, x2min], [x1max, x2max])
    min_res = minimize(
        get_feedback_sparcity_error,
        np.array([5, 500]),
        method="Nelder-Mead",
        args=(df),
        bounds=bounds,
        options={"maxiter": 10, "ftol": 0.1, "xtol": 0.1},
    )

    return int(min_res.x[0]), int(min_res.x[1])


def get_weighted_interactions(storage_path: str):

    bookings = weighted_interactions(
        pd.read_csv(f"{storage_path}/raw_data_bookings.csv")
    )
    clicks = weighted_interactions(pd.read_csv(f"{storage_path}/raw_data_clicks.csv"))
    favorites = weighted_fav(pd.read_csv(f"{storage_path}/raw_data_favorites.csv"))
    return bookings, clicks, favorites


def get_offers_and_users_to_keep(
    grouped_df, minimal_user_strength, minimal_offer_strength
):
    grouped_df_offer_sum = grouped_df.groupby(["offer_id"], as_index=False)[
        "eventStrength"
    ].sum()
    grouped_df_user_sum = grouped_df.groupby(["user_id"], as_index=False)[
        "eventStrength"
    ].sum()

    cleaned_df_user_relevant = grouped_df_user_sum.query(
        f"eventStrength >{minimal_user_strength}"
    )
    cleaned_df_offer_relevant = grouped_df_offer_sum.query(
        f"eventStrength >{minimal_offer_strength}"
    )
    list_user_to_keep = cleaned_df_user_relevant.user_id.tolist()
    list_offer_to_keep = cleaned_df_offer_relevant.offer_id.tolist()
    return list_user_to_keep, list_offer_to_keep


def get_EAC_feedback(df_cleaned):
    return (
        df_cleaned.query("user_age == 15"),
        df_cleaned.query("user_age == 16"),
        df_cleaned.query("user_age == 17"),
    )


def csr_vappend(a, b):
    """Takes in 2 csr_matrices and appends the second one to the bottom of the first one.
    Much faster than scipy.sparse.vstack but assumes the type to be csr and overwrites
    the first matrix instead of copying it. The data, indices, and indptr still get copied."""

    a.data = np.hstack((a.data, b.data))
    a.indices = np.hstack((a.indices, b.indices))
    a.indptr = np.hstack((a.indptr, (b.indptr + a.nnz)[1:]))
    a._shape = (a.shape[0] + b.shape[0], b.shape[1])
    return a


def add_CS_users_and_get_user_list(fm, storage_path):
    # Building EAC CS users
    df_u15 = pd.read_csv(f"{storage_path}/clean_data_u15.csv")
    df_u16 = pd.read_csv(f"{storage_path}/clean_data_u16.csv")
    df_u17 = pd.read_csv(f"{storage_path}/clean_data_u17.csv")
    sparce_matrice_u15, user_list_u15, item_list = get_meta_and_sparse(df_u15)
    sparce_matrice_u16, user_list, item_list = get_meta_and_sparse(df_u16)
    sparce_matrice_u17, user_list, item_list = get_meta_and_sparse(df_u17)
    mean_axis_u15 = sparce_matrice_u15.sum(axis=0) / sparce_matrice_u15.getnnz(axis=0)
    mean_axis_u16 = sparce_matrice_u16.sum(axis=0) / sparce_matrice_u16.getnnz(axis=0)
    mean_axis_u17 = sparce_matrice_u17.sum(axis=0) / sparce_matrice_u17.getnnz(axis=0)
    sp_u_15 = sparse.csr_matrix(mean_axis_u15)
    sp_u_16 = sparse.csr_matrix(mean_axis_u16)
    sp_u_17 = sparse.csr_matrix(mean_axis_u17)
    spu_15_16 = csr_vappend(sp_u_15, sp_u_16)
    spu_15_16_17 = csr_vappend(spu_15_16, sp_u_17)
    # Building +18 CS user
    mean_axis0 = fm.sum(axis=0) / fm.getnnz(axis=0)
    sp_cs_user = sparse.csr_matrix(mean_axis0)
    sp_train_cold_start = csr_vappend(sp_cs_user, spu_15_16_17)
    df_train_with_cs_user = csr_vappend(sp_train_cold_start, fm)
    eac_user_list = np.array(["eac15", "eac16", "eac17"])
    return df_train_with_cs_user, eac_user_list
