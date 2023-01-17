import tensorflow as tf
import pandas as pd

import shutil
from loguru import logger

from sklearn.decomposition import PCA
import matplotlib as mpl
import matplotlib.pyplot as plt


def save_pca_representation(
    loaded_model: tf.keras.models.Model,
    item_data: pd.DataFrame,
    figures_folder: str,
):
    # We remove the first element, the [UNK] token
    item_ids = loaded_model.item_layer.layers[0].get_vocabulary()[1:]
    embeddings = loaded_model.item_layer.layers[1].get_weights()[0][1:]

    pca_out = PCA(n_components=2).fit_transform(embeddings)
    categories = item_data["offer_categoryId"].unique().tolist()
    item_representation = pd.DataFrame(
        {
            "item_id": item_ids,
            "x": pca_out[:, 0],
            "y": pca_out[:, 1],
        }
    ).merge(item_data, on=["item_id"], how="inner")

    colormap = mpl.colormaps["tab20"].colors
    fig, ax = plt.subplots(1, 1, figsize=(15, 10))
    for idx, category in enumerate(categories):
        data = item_representation.loc[lambda df: df["offer_categoryId"] == category]
        ax.scatter(
            data["x"].values,
            data["y"].values,
            s=10,
            color=colormap[idx],
            label=category,
            alpha=0.7,
        )

        logger.info(f"Plotting {len(data)} points for category {category}")
        fig_sub, ax_sub = plt.subplots(1, 1, figsize=(15, 10))
        for idx_sub, subcategory in enumerate(data["offer_subcategoryid"].unique()):
            data_sub = data.loc[lambda df: df["offer_subcategoryid"] == subcategory]
            ax_sub.scatter(
                data_sub["x"].values,
                data_sub["y"].values,
                s=10,
                color=colormap[idx_sub],
                label=subcategory,
                alpha=0.7,
            )
        ax_sub.legend()
        ax_sub.grid(True)
        fig_sub.savefig(figures_folder + f"{category}.pdf")

    ax.legend()
    ax.grid(True)
    fig.savefig(figures_folder + "ALL_CATEGORIES.pdf")


def remove_dir(path):
    shutil.rmtree(path)
