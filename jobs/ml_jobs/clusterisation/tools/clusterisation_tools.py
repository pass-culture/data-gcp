import pandas as pd
import scipy.cluster.hierarchy as sch
from scipy.cluster.hierarchy import fcluster
from sklearn.preprocessing import StandardScaler
import umap
import bokeh.io
import bokeh.palettes
import bokeh.plotting
import scipy.cluster.hierarchy
import bdendro
from bokeh.plotting import figure, output_file, save
from bokeh.plotting import figure, output_file, show
import hvplot.pandas  # noqa

pd.options.plotting.backend = "hvplot"

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
    cat, item_full_encoding_enriched_given_cat, target_nbclusters, output_table
):
    # Here we need the dataframe with 1 col by emb dim
    item_full_encoding_by_component_given_cat = item_full_encoding_enriched_given_cat[
        ["t0", "t1", "t2", "t3", "t4", "c0", "c1"]
    ].values.tolist()
    scaler.fit(item_full_encoding_by_component_given_cat)
    item_full_encoding_by_component_given_cat_std = scaler.transform(
        item_full_encoding_by_component_given_cat
    )

    group_embedding_dist = []
    item_full_encoding_by_component_given_cat_reduced_2d = pd.DataFrame()
    reduce_pretrained_emb = True
    if not reduce_pretrained_emb:
        group_embedding_dist = item_full_encoding_by_component_given_cat.values.tolist()
    else:
        if clusterisation_reduct_method == "umap":
            red2 = umap.UMAP(
                n_components=clusterisation_reduction,
                random_state=42,
                n_neighbors=10,
                transform_seed=42,
                verbose=True,
            )
            red2.fit(item_full_encoding_by_component_given_cat_std)
            X2 = red2.transform(item_full_encoding_by_component_given_cat_std)
        if clusterisation_reduct_method == "pca":
            red_pca = PCA(n_components=clusterisation_reduction)
            red_pca.fit(item_full_encoding_by_component_given_cat_std)
            X2 = red_pca.transform(item_full_encoding_by_component_given_cat_std)
        if clusterisation_reduct_method == "tsne":
            X2 = TSNE(
                n_components=clusterisation_reduction,
                learning_rate="auto",
                init="random",
                perplexity=3,
            ).fit_transform(item_full_encoding_by_component_given_cat_std)

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
                item_full_encoding_by_component_given_cat_reduced_2d.reset_index(
                    drop=True
                ),
                item_full_encoding_enriched_given_cat.reset_index(drop=True),
            ],
            axis=1,
        )

        group_embedding_dist = (
            item_full_encoding_by_component_given_cat_reduced_2d_enriched[
                ["x", "y"]
            ].values.tolist()
        )

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

    reduction_super = 2
    target = dfclusters["cluster"].values.tolist()
    red_super = umap.UMAP(
        n_components=reduction_super,
        random_state=42,
        n_neighbors=10,
        transform_seed=42,
        verbose=True,
    )
    X_super = red_super.fit_transform(
        item_full_encoding_by_component_given_cat_std, y=target
    )
    item_full_encoding_by_component_given_cat_super_reduced_2d = pd.DataFrame(X_super)
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
            item_full_encoding_by_component_given_cat_reduced_2d_enriched.reset_index(
                drop=True
            ),
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
