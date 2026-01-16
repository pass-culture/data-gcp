from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
from loguru import logger
from prophet import Prophet


def plot_prophet_changepoints(
    model: Prophet,
    df_train: pd.DataFrame,
    title: str = "Prophet Changepoints on Train Set",
    figsize: tuple = (12, 6),
):
    """
    Returns a matplotlib figure showing training data and changepoints.
    Args:
        model: Trained Prophet model.
        df_train: DataFrame used for training with 'ds' and 'y' columns.
        title: Title of the plot.
        figsize: Figure size tuple.
    Returns:
        Matplotlib figure object.
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
    forecast_train: pd.DataFrame,
    changepoints: list,
    figsize: tuple = (10, 6),
):
    """
    Returns a matplotlib figure of trend with changepoints.
    Args:
        forecast_train: DataFrame from model.predict on training data.
        changepoints: List of changepoint dates.
        figsize: Figure size tuple.
    Returns:
        Matplotlib figure object.
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


def plot_cv_results(
    perf: pd.DataFrame, metrics: list, output_dir: str | Path = "cv_plots"
) -> list[str]:
    """
    Plots cross-validation results for specified metrics and saves figures.

    Args:
        perf: DataFrame with cross-validation performance metrics.
        metrics: List of metric names to plot.
        output_dir: Directory to save the plots (str or Path).

    Returns:
        List of saved plot file paths
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

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

            plot_path = output_path / f"cv_{m}.png"
            fig.savefig(plot_path)
            plt.close(fig)

            plot_paths.append(str(plot_path))

    return plot_paths


def plot_forecast_vs_actuals(
    forecast: pd.DataFrame,
    freq: str,
    title: str = "Forecast vs Actuals",
    y_label: str = "Pricing €",
    figsize: tuple = (12, 8),
):
    """
    Returns a matplotlib figure comparing forecast vs actuals.

    Args:
        forecast: DataFrame with columns:
            ds, y, yhat, yhat_lower, yhat_upper
        freq: Frequency string ('D', 'W-MON', etc.)
    """
    if freq == "W-MON":
        x_label = "weeks"
    elif freq == "D":
        x_label = "days"
    else:
        x_label = "date"

    fig, ax = plt.subplots(figsize=figsize)

    ax.plot(
        forecast.ds,
        forecast.y,
        label="actuals",
    )
    ax.plot(
        forecast.ds,
        forecast.yhat,
        label="forecast (yhat)",
    )

    ax.plot(
        forecast.ds,
        forecast.yhat_upper,
        linestyle="--",
        label="yhat upper",
        alpha=0.6,
    )
    ax.plot(
        forecast.ds,
        forecast.yhat_lower,
        linestyle="--",
        label="yhat lower",
        alpha=0.6,
    )

    ax.fill_between(
        forecast.ds,
        forecast.yhat_lower,
        forecast.yhat_upper,
        alpha=0.3,
        label="uncertainty interval",
    )

    ax.set_title(title)
    ax.set_xlabel(x_label)
    ax.set_ylabel(y_label)
    ax.legend()

    fig.tight_layout()

    return fig


def log_diagnostic_plots(model: Prophet, df_train: pd.DataFrame) -> dict:
    """Generate diagnostic plots for the Prophet model.

    Args:
        model: Trained Prophet model.
        df_train: DataFrame used for training with 'ds' and 'y' columns.

    Returns:
        Dictionary containing:
            - 'changepoints': Figure showing changepoints on training data
            - 'trend': Figure showing trend with changepoints
            - 'components': Figure showing Prophet components
    """
    logger.info("Generating diagnostic plots")

    fig_cp = plot_prophet_changepoints(model, df_train)
    forecast_train = model.predict(df_train)
    fig_trend = plot_trend_with_changepoints(forecast_train, model.changepoints)
    fig_components = model.plot_components(forecast_train)

    logger.info("Diagnostic plots generated successfully")

    return {
        "changepoints": fig_cp,
        "trend": fig_trend,
        "components": fig_components,
    }
