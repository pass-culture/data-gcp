import pandas as pd
import scipy.cluster.hierarchy as sch
from scipy.cluster.hierarchy import fcluster
from sklearn.preprocessing import StandardScaler
import umap
import joblib
from loguru import logger

# pd.options.plotting.backend = "hvplot"

import holoviews as hv
from holoviews import opts, dim

hv.extension("bokeh")

from tools.utils import (
    ENV_SHORT_NAME,
    clusterisation_reduction,
    clusterisation_reduct_method,
)

scaler = StandardScaler()


def clusterisation_from_prebuild_encoding(
    item_full_encoding_enriched_given_cat, target_nbclusters, splitting
):
    # Here we need the dataframe with 1 col by emb dim
    item_full_encoding_enriched_given_cat[
        "rank"
    ] = item_full_encoding_enriched_given_cat["rank"].astype(int)
    item_full_encoding_by_component_given_cat = item_full_encoding_enriched_given_cat[
        ["t0", "t1", "t2", "t3", "t4", "c0", "c1"]
    ].values.tolist()

    (
        item_full_encoding_by_component_given_cat_fitted,
        item_full_encoding_by_component_given_cat_predicted,
    ) = split_data(item_full_encoding_enriched_given_cat, splitting)

    item_full_encoding_by_component_given_cat_fitted_std, item_full_encoding_by_component_given_cat_predicted_std = scale_data(item_full_encoding_by_component_given_cat, item_full_encoding_by_component_given_cat_fitted, item_full_encoding_by_component_given_cat_predicted)

    item_full_encoding_by_component_given_cat_std = pd.concat(
        [
            item_full_encoding_by_component_given_cat_fitted_std,
            item_full_encoding_by_component_given_cat_predicted_std,
        ],
        ignore_index=True,
    )

    group_embedding_dist = []
    ReducePretrainedEmb = False
    if not ReducePretrainedEmb:
        group_embedding_dist = item_full_encoding_by_component_given_cat_std
    else:
        group_embedding_dist = reduce_pretrained_embedding(
            item_full_encoding_enriched_given_cat,
            item_full_encoding_by_component_given_cat_std,
        )

    dfclusters = get_clusters_from_linkage_matrix(
        target_nbclusters, group_embedding_dist
    )

    item_full_encoding_by_component_given_cat_super_reduced_2d = (
        get_clusters_2D_coordinates(
            item_full_encoding_by_component_given_cat_fitted_std,
            item_full_encoding_by_component_given_cat_predicted_std,
            dfclusters,
        )
    )

    dfclusters_enriched = clustering_postprocess(
        item_full_encoding_enriched_given_cat,
        dfclusters,
        item_full_encoding_by_component_given_cat_super_reduced_2d,
    )
    return dfclusters_enriched

def scale_data(item_full_encoding_by_component_given_cat, item_full_encoding_by_component_given_cat_fitted, item_full_encoding_by_component_given_cat_predicted):
    scaler.fit(item_full_encoding_by_component_given_cat)
    # fullsize = len(item_full_encoding_by_component_given_cat)
    # split = int(fullsize / 2)

    item_full_encoding_by_component_given_cat_fitted_std = scaler.transform(
        item_full_encoding_by_component_given_cat_fitted
    )

    item_full_encoding_by_component_given_cat_predicted_std = scaler.transform(
        item_full_encoding_by_component_given_cat_predicted
    )
    
    return item_full_encoding_by_component_given_cat_fitted_std,item_full_encoding_by_component_given_cat_predicted_std


def clustering_postprocess(
    item_full_encoding_enriched_given_cat,
    dfclusters,
    item_full_encoding_by_component_given_cat_super_reduced_2d,
):
    item_full_encoding_by_component_given_cat_super_reduced_2d.columns = [
        str(column)
        for column in item_full_encoding_by_component_given_cat_super_reduced_2d.columns
    ]
    item_full_encoding_by_component_given_cat_super_reduced_2d = (
        item_full_encoding_by_component_given_cat_super_reduced_2d.rename(
            columns={"0": "x_super", "1": "y_super"}
        )
    )

    item_full_encoding_by_component_given_cat_reduced_2d = pd.concat(
        [
            item_full_encoding_enriched_given_cat.reset_index(drop=True),
            item_full_encoding_by_component_given_cat_super_reduced_2d.reset_index(
                drop=True
            ),
        ],
        axis=1,
    )

    dfclusters_enriched = pd.concat(
        [
            dfclusters[["cluster"]].reset_index(drop=True),
            item_full_encoding_by_component_given_cat_reduced_2d[
                ["item_id"]
            ].reset_index(drop=True),
        ],
        axis=1,
    ).reset_index()
    dfclusters_enriched["cluster"] = dfclusters_enriched["cluster"].astype("string")

    item_full_encoding_enriched_given_cat["semantic_encoding"] = (
        item_full_encoding_enriched_given_cat[["t0", "t1", "t2", "t3", "t4"]]
        .to_numpy()
        .tolist()
    )
    item_full_encoding_enriched_given_cat["categorical_encoding"] = (
        item_full_encoding_enriched_given_cat[["c0", "c1"]].to_numpy().tolist()
    )
    item_full_encoding_enriched_given_cat = item_full_encoding_enriched_given_cat.drop(
        ["t0", "t1", "t2", "t3", "t4", "c0", "c1"], axis=1
    )

    label_list = [
        "category",
        "subcategory_id",
        "offer_sub_type_label",
        "offer_type_label",
    ]
    item_full_encoding_enriched_given_cat[
        "label"
    ] = item_full_encoding_enriched_given_cat[label_list].agg("_".join, axis=1)
    item_full_encoding_enriched_given_cat[
        "semantic_encoding"
    ] = item_full_encoding_enriched_given_cat["semantic_encoding"].astype(str)
    item_full_encoding_enriched_given_cat[
        "categorical_encoding"
    ] = item_full_encoding_enriched_given_cat["categorical_encoding"].astype(str)
    item_full_encoding_enriched_given_cat = item_full_encoding_enriched_given_cat[
        ["item_id", "label", "semantic_encoding", "categorical_encoding"]
    ]
    dfclusters_enriched = dfclusters_enriched.drop_duplicates(
        subset=["item_id"], keep="first"
    )
    item_full_encoding_enriched_given_cat = (
        item_full_encoding_enriched_given_cat.drop_duplicates(
            subset=["item_id"], keep="first"
        )
    )
    dfclusters_enriched = pd.merge(
        dfclusters_enriched,
        item_full_encoding_enriched_given_cat,
        how="inner",
        on=["item_id"],
    )
    dfclusters_enriched = dfclusters_enriched.drop_duplicates(
        subset=["item_id"], keep="first"
    )

    return dfclusters_enriched


def get_clusters_2D_coordinates(
    item_full_encoding_by_component_given_cat_fitted_std,
    item_full_encoding_by_component_given_cat_predicted_std,
    dfclusters,
):
    reduction_super = 2
    target = dfclusters["cluster"].values.tolist()
    red_super = umap.UMAP(
        n_components=reduction_super,
        random_state=42,
        n_neighbors=10,
        transform_seed=42,
        verbose=True,
    )
    logger.info("fit_transform: fitted data...")
    X_super = red_super.fit_transform(
        item_full_encoding_by_component_given_cat_fitted_std, y=target
    )

    logger.info("Transform: predicted data..")
    X_super_predicted = red_super.transform(
        item_full_encoding_by_component_given_cat_predicted_std
    )

    item_full_encoding_by_component_given_cat_super_reduced_2d_fitted = pd.DataFrame(
        X_super
    )
    item_full_encoding_by_component_given_cat_super_reduced_2d_predict = pd.DataFrame(
        X_super_predicted
    )
    item_full_encoding_by_component_given_cat_super_reduced_2d = pd.concat(
        [
            item_full_encoding_by_component_given_cat_super_reduced_2d_fitted,
            item_full_encoding_by_component_given_cat_super_reduced_2d_predict,
        ],
        ignore_index=True,
    )

    return item_full_encoding_by_component_given_cat_super_reduced_2d


def reduce_pretrained_embedding(
    item_full_encoding_enriched_given_cat, item_full_encoding_by_component_given_cat_std
):
    red2 = umap.UMAP(
        n_components=clusterisation_reduction,
        random_state=42,
        n_neighbors=10,
        transform_seed=42,
        verbose=True,
    )
    red2.fit(item_full_encoding_by_component_given_cat_std)
    X2 = red2.transform(item_full_encoding_by_component_given_cat_std)

    item_full_encoding_by_component_given_cat_reduced_2d = pd.DataFrame(X2)
    item_full_encoding_by_component_given_cat_reduced_2d.columns = [
        str(column)
        for column in item_full_encoding_by_component_given_cat_reduced_2d.columns
    ]
    item_full_encoding_by_component_given_cat_reduced_2d = (
        item_full_encoding_by_component_given_cat_reduced_2d.rename(
            columns={"0": "x", "1": "y"}
        )
    )
    item_full_encoding_by_component_given_cat_reduced_2d_enriched = pd.concat(
        [
            item_full_encoding_by_component_given_cat_reduced_2d.reset_index(drop=True),
            item_full_encoding_enriched_given_cat.reset_index(drop=True),
        ],
        axis=1,
    )

    group_embedding_dist = (
        item_full_encoding_by_component_given_cat_reduced_2d_enriched[
            ["x", "y"]
        ].values.tolist()
    )

    return group_embedding_dist


def get_clusters_from_linkage_matrix(target_nbclusters, group_embedding_dist):
    linkage_matrix = sch.linkage(group_embedding_dist, method="ward")
    nbclusters = []
    for thr in range(0, 150, 5):
        threshold = thr
        clusters = fcluster(linkage_matrix, threshold, criterion="distance")
        nbclusters.append([thr, max(clusters)])
    dfnbclusters = pd.DataFrame(nbclusters)
    dfnbclusters.columns = ["threshold", "number of clusters"]

    dfnbclusters_best = dfnbclusters.loc[
        dfnbclusters["number of clusters"] < target_nbclusters
    ]
    best_threshold = dfnbclusters_best[["threshold"]].head(1).values.tolist()[0][0]

    clusters = fcluster(linkage_matrix, best_threshold, criterion="distance")
    dfclusters = pd.DataFrame(clusters)
    dfclusters = dfclusters.rename(columns={0: "cluster"})
    return dfclusters


def split_data(item_full_encoding_enriched_given_cat, splitting):
    splitt_row_number = int(
        (len(item_full_encoding_enriched_given_cat) + 1) * (int(splitting) / 100)
    )
    item_full_encoding_by_component_given_cat_fitted = (
        item_full_encoding_enriched_given_cat.drop(["rank"], axis=1).iloc[
            :splitt_row_number
        ]
    )
    item_full_encoding_by_component_given_cat_predicted = (
        item_full_encoding_enriched_given_cat.drop(["rank"], axis=1).iloc[
            splitt_row_number:
        ]
    )

    item_full_encoding_by_component_given_cat_fitted = (
        item_full_encoding_by_component_given_cat_fitted[
            ["t0", "t1", "t2", "t3", "t4", "c0", "c1"]
        ].values.tolist()
    )

    item_full_encoding_by_component_given_cat_predicted = (
        item_full_encoding_by_component_given_cat_predicted[
            ["t0", "t1", "t2", "t3", "t4", "c0", "c1"]
        ].values.tolist()
    )

    return (
        item_full_encoding_by_component_given_cat_fitted,
        item_full_encoding_by_component_given_cat_predicted,
    )
