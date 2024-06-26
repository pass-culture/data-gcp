import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from sklearn.metrics import confusion_matrix


def plot_features_importance(pipeline, filename):
    feature_importance = pd.DataFrame(
        {
            "feature": pipeline.model.feature_name()[:20],
            "importance": pipeline.model.feature_importance()[:20],
        }
    )

    feature_importance = (
        feature_importance.groupby(["feature"])["importance"].sum().reset_index()
    )
    feature_importance = feature_importance.sort_values(
        by="importance", ascending=False
    )
    plt.figure(figsize=(10, 6))
    sns.barplot(x="importance", y="feature", data=feature_importance)
    plt.xlabel("Feature Importance")
    plt.title("Feature Importance Plot")
    plt.savefig(filename, format="pdf", dpi=300, bbox_inches="tight")
    plt.close()


def plot_cm(
    y,
    y_pred,
    filename,
    perc=True,
    proba=0.5,
):
    y_pred_binary = (y_pred >= proba).astype(int)
    y_true_binary = (y >= proba).astype(int)

    conf_matrix = confusion_matrix(y_true_binary, y_pred_binary)
    print(conf_matrix)

    plt.figure(figsize=(6, 4))
    if perc:
        conf_matrix_percent = (
            conf_matrix.astype("float") / conf_matrix.sum(axis=1)[:, np.newaxis]
        )
        sns.heatmap(conf_matrix_percent, annot=True, fmt=".2f")
    else:
        sns.heatmap(conf_matrix, annot=True, fmt=".0f")

    plt.xlabel("Predicted Label")
    plt.ylabel("True Label")
    plt.title(f"Confusion Matrix [proba >= {proba}]")

    plt.savefig(filename, format="pdf", dpi=300, bbox_inches="tight")

    plt.close()


def plot_hist(df, figure_folder, prefix=""):
    ax = df["target"].hist(bins=30, histtype="barstacked", stacked=True)
    fig = ax.get_figure()
    fig.savefig(f"{figure_folder}/{prefix}target_histogram.pdf")
