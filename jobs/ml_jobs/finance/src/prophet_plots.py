import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.path import Path


def plot_prophet_changepoints(
    model,
    df_train,
    title="Prophet Changepoints on Train Set",
    figsize=(12, 6),
):
    """
    Returns a matplotlib figure showing training data and changepoints.
    """
    fig, ax = plt.subplots(figsize=figsize)

    ax.plot(df_train.ds, df_train.y, label="train")

    for cp in model.changepoints:
        ax.axvline(cp, color="red", linestyle="--", alpha=0.7)

    ax.set_title(f"{title}\nNumber of changepoints: {len(model.changepoints)}")
    ax.set_xlabel("Date")
    ax.set_ylabel("y")
    ax.legend()
    fig.tight_layout()

    return fig


def plot_trend_with_changepoints(
    forecast_train,
    changepoints,
    figsize=(10, 6),
):
    """
    Returns a matplotlib figure of trend with changepoints.
    """
    fig, ax = plt.subplots(figsize=figsize)

    ax.plot(forecast_train.ds, forecast_train["trend"], label="trend")

    for cp in changepoints:
        cp_dt = pd.to_datetime(cp)
        ax.axvline(cp_dt, color="orange", linestyle="--", alpha=0.8)

        ax.text(
            cp_dt,
            ax.get_ylim()[1] * 0.95,
            str(cp),
            rotation=90,
            color="orange",
            va="top",
            ha="right",
            fontsize=9,
        )

    ax.set_title("Trend with Changepoints")
    ax.set_xlabel("Date")
    ax.set_ylabel("Trend")
    ax.legend()
    fig.tight_layout()

    return fig


def plot_cv_results(perf, metrics, output_dir="cv_plots"):
    """
    Plots cross-validation results for specified metrics and saves figures.

    Returns:
        List of saved plot file paths
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    plot_paths = []

    for m in metrics:
        if m in perf.columns:
            fig, ax = plt.subplots(figsize=(8, 4))
            ax.plot(perf["horizon"], perf[m], marker="o", linestyle="-", label=m)

            ax.set_xlabel("Horizon")
            ax.set_ylabel(m.upper())
            ax.set_title(f"Prophet CV: {m.upper()} by Horizon")
            ax.legend()
            fig.tight_layout()

            plot_path = output_dir / f"cv_{m}.png"
            fig.savefig(plot_path)
            plt.close(fig)  # IMPORTANT

            plot_paths.append(str(plot_path))

    return plot_paths
