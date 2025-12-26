import matplotlib.pyplot as plt
import pandas as pd


def plot_prophet_changepoints(
    model, df_train, title="Prophet Changepoints on Train Set", figsize=(12, 6)
):
    """
    Plots the training set with Prophet changepoints
    and displays the number of changepoints.
    Args:
        model: Trained Prophet model.
        df_train: DataFrame with training data (columns: ds, y).
        title: Plot title.
        figsize: Figure size.
    """

    plt.figure(figsize=figsize)
    plt.plot(df_train.ds, df_train.y, label="train", color="tab:green")
    for cp in model.changepoints:
        plt.axvline(cp, color="red", linestyle="--", alpha=0.7)
    plt.title(f"{title}\nNumber of changepoints: {len(model.changepoints)}")
    plt.xlabel("Date")
    plt.ylabel("y")
    plt.legend()
    plt.tight_layout()
    plt.show()


def plot_trend_with_changepoints(results, changepoints):
    plt.figure(figsize=(10, 6))
    plt.plot(results["forecast_train"].ds, results["forecast_train"]["trend"])
    # Annotate changepoints
    # Add vertical lines for changepoints
    for cp in changepoints:
        plt.axvline(
            pd.to_datetime(cp),
            color="orange",
            linestyle="--",
            alpha=0.8,
        )

    for cp in changepoints:
        plt.text(
            pd.to_datetime(cp),
            plt.ylim()[1] * 0.95,
            cp,
            rotation=90,
            color="orange",
            va="top",
            ha="right",
            fontsize=9,
        )
    plt.title("Trend with Changepoints")
    plt.xlabel("Date")
    plt.ylabel("Trend")
    plt.show()
