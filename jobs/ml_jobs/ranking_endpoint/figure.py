import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from sklearn.metrics import confusion_matrix, f1_score


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
    y: pd.Series,
    y_pred: pd.Series,
    filename: str,
    perc: bool,
    proba: False,
):
    y_pred_binary = (y_pred >= proba).astype(int)
    y_true_binary = (y >= proba).astype(int)

    plt.figure(figsize=(6, 4))
    conf_matrix = confusion_matrix(y_true_binary, y_pred_binary)
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


def compute_y_binary(
    y_pred_consulted_series: pd.Series,
    y_pred_booked_series: pd.Series,
    perc_consulted: float,
    perc_booked: float,
) -> pd.Series:
    # Vectorized classification logic
    y_pred_booked = y_pred_booked_series.values
    y_pred_consulted = y_pred_consulted_series.values

    # Create boolean masks for the conditions
    mask_booked = y_pred_booked >= perc_booked
    mask_consulted = y_pred_consulted >= perc_consulted
    mask_ratio = (y_pred_booked / perc_booked) > (y_pred_consulted / perc_consulted)

    # Initialize the result array with zeros
    y_pred_binary = np.zeros_like(y_pred_booked, dtype=int)

    # Apply the conditions
    y_pred_binary[mask_booked & mask_ratio] = 2
    y_pred_binary[mask_booked & ~mask_ratio] = 1
    y_pred_binary[~mask_booked & mask_consulted] = 1

    # Convert the result back to a pandas Series
    return pd.Series(y_pred_binary, index=y_pred_booked_series.index)


def plot_cm_multiclass(
    y_true: pd.Series,
    y_pred_consulted: pd.Series,
    y_pred_booked: pd.Series,
    perc_consulted: float,
    perc_booked: float,
    filename,
    class_names,
):
    y_pred_binary = compute_y_binary(
        y_pred_consulted, y_pred_booked, perc_consulted, perc_booked
    )

    conf_matrix = confusion_matrix(y_true, y_pred_binary)
    print("Confusion matrix:")
    print(f"proba_consult={perc_consulted:.3f}, proba_booked={perc_consulted:.3f}")
    print(conf_matrix)
    print("----")

    # Compute F1 score globally, then for each class
    f1_per_class = f1_score(y_true, y_pred_binary, average=None)
    f1_global_weighted = f1_score(y_true, y_pred_binary, average="weighted")
    f1_global_macro = f1_score(y_true, y_pred_binary, average="macro")
    print(f"Global F1 score (weighted): {f1_global_weighted:.2f}")
    print(f"Global F1 score (macro): {f1_global_macro:.2f}")
    print("F1 score for each class:")
    for i, score in enumerate(f1_per_class):
        print(f"Class {i}: {score:.2f}")

    # Draw the confusion matrix
    plt.figure(figsize=(8, 6))
    conf_matrix_percent = (
        conf_matrix.astype("float") / conf_matrix.sum(axis=1)[:, np.newaxis]
    )
    sns.heatmap(
        conf_matrix_percent,
        annot=True,
        fmt=".2f",
        xticklabels=class_names,
        yticklabels=class_names,
    )
    plt.xlabel("Predicted Label")
    plt.ylabel("True Label")
    plt.title(
        f"Confusion Matrix. f1 weighted: {f1_global_weighted:.2f}, f1 macro: {f1_global_macro:.2f}"
    )
    plt.savefig(filename, format="pdf", dpi=300, bbox_inches="tight")
    plt.close()


def plot_hist(df, figure_folder, prefix=""):
    ax = df["target"].hist(bins=30, histtype="barstacked", stacked=True)
    fig = ax.get_figure()
    fig.savefig(f"{figure_folder}/{prefix}target_histogram.pdf")


def plot_regression_figures(
    regression_target: pd.Series,
    regression_score: pd.Series,
    figure_folder: str,
    prefix="",
):
    # Residuals
    residuals = regression_target - regression_score

    # Plot Residuals
    filename = f"{figure_folder}/{prefix}residuals_plot.png"
    plt.figure(figsize=(10, 5))
    plt.scatter(regression_score, residuals, alpha=0.5)
    plt.axhline(y=0, color="r", linestyle="--")
    plt.title("Residuals Plot")
    plt.xlabel("Predicted Values")
    plt.ylabel("Residuals")
    plt.savefig(filename, format="png", dpi=100, bbox_inches="tight")
    plt.close()

    # Regression Plot
    filename = f"{figure_folder}/{prefix}regression_plot.png"
    plt.figure(figsize=(10, 5))
    plt.scatter(regression_target, regression_score, alpha=0.5)
    plt.plot([0, 5], [0, 5], color="r", linestyle="--")
    plt.title("Predicted vs Actual")
    plt.xlabel("Actual Values")
    plt.ylabel("Predicted Values")
    plt.savefig(filename, format="png", dpi=100, bbox_inches="tight")
    plt.close()

    # Histogram of Actual vs Predicted Values
    filename = f"{figure_folder}/{prefix}histogram_actual_vs_predicted.pdf"
    plt.figure(figsize=(10, 5))
    plt.hist(regression_target, bins=40, alpha=0.5, label="Actual")
    plt.hist(regression_score, bins=40, alpha=0.5, label="Predicted")
    plt.title("Histogram of Actual vs Predicted Values")
    plt.xlabel("Value")
    plt.ylabel("Frequency")
    plt.legend()
    plt.savefig(filename, format="pdf", dpi=300, bbox_inches="tight")
    plt.close()

    # Histogram of Residuals
    filename = f"{figure_folder}/{prefix}histogram_residuals.pdf"
    plt.figure(figsize=(10, 5))
    plt.hist(residuals, bins=40, alpha=0.5, color="g")
    plt.title("Histogram of Residuals")
    plt.xlabel("Residuals")
    plt.ylabel("Frequency")
    plt.savefig(filename, format="pdf", dpi=300, bbox_inches="tight")
    plt.close()
