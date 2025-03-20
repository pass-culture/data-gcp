import secrets

import matplotlib.pyplot as plt
import pandas as pd
import tensorflow as tf
from loguru import logger
from sklearn.decomposition import PCA


def save_pca_representation(
    loaded_model: tf.keras.models.Model,
    item_data: pd.DataFrame,
    figures_folder: str,
) -> None:
    """
    Computes a 2D PCA projection of item embeddings from the retrieval model
    and generates scatter plots for visualizing item distributions by category and subcategory.

    Args:
        loaded_model (tf.keras.models.Model): Trained retrieval model from which item embeddings are extracted.
        item_data (pd.DataFrame): DataFrame containing metadata for items, including 'item_id',
            'offer_category_id', and 'offer_subcategory_id'.
        figures_folder (str): Path to the folder where the generated plots will be saved.

    Returns:
        None. Saves visualizations as PDF files in the specified folder.
    """
    item_ids = loaded_model.item_layer.layers[0].get_vocabulary()[1:]
    embeddings = loaded_model.item_layer.layers[1].get_weights()[0][1:]

    seed = secrets.randbelow(1000)
    logger.info(f"Random state for PCA fixed to seed={seed}")
    pca_out = PCA(n_components=2, random_state=seed).fit_transform(embeddings)
    categories = item_data["offer_category_id"].unique().tolist()
    item_representation = pd.DataFrame(
        {
            "item_id": item_ids,
            "x": pca_out[:, 0],
            "y": pca_out[:, 1],
        }
    ).merge(item_data, on=["item_id"], how="inner")

    colormap = plt.cm.tab20.colors
    fig, ax = plt.subplots(1, 1, figsize=(15, 10))
    for idx, category in enumerate(categories):
        data = item_representation.loc[lambda df: df["offer_category_id"] == category]
        max_plots = min(data.shape[0], 10000)
        data = data.sample(n=max_plots)
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
        for idx_sub, subcategory in enumerate(data["offer_subcategory_id"].unique()):
            data_sub = data.loc[lambda df: df["offer_subcategory_id"] == subcategory]
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


def plot_metrics_evolution(metrics, list_k, figures_folder):
    """
    Plot the evolution of metrics with different k values

    Args:
        metrics: Dictionary containing metrics for different k values
        figures_folder: Folder to save the plot

    Returns:
        None
    """
    logger.info("Creating metrics evolution plot")

    # Prepare data for plotting
    precision_values = [
        metrics[metric_name]
        for metric_name in metrics.keys()
        if metric_name.startswith("precision")
    ]
    recall_values = [
        metrics[metric_name]
        for metric_name in metrics.keys()
        if metric_name.startswith("recall")
    ]
    coverage_values = [
        metrics[metric_name]
        for metric_name in metrics.keys()
        if metric_name.startswith("coverage")
    ]
    novelty_values = [
        metrics[metric_name]
        for metric_name in metrics.keys()
        if metric_name.startswith("novelty")
    ]

    # Create plot
    fig, ax = plt.subplots(figsize=(12, 8))

    # Plot each metric
    ax.plot(list_k, precision_values, marker="o", label="Precision")
    ax.plot(list_k, recall_values, marker="s", label="Recall")
    ax.plot(list_k, coverage_values, marker="^", label="Coverage")
    ax.plot(list_k, novelty_values, marker="d", label="Novelty")

    ax.set_xlabel("k (Number of recommendations)")
    ax.set_ylabel("Score")
    ax.set_title("Evolution of Metrics with k")
    ax.grid(True)
    ax.legend()

    # Save the plot
    plot_path = f"{figures_folder}/metrics_evolution.png"
    fig.savefig(plot_path)
    logger.info(f"Metrics evolution plot saved to {plot_path}")
