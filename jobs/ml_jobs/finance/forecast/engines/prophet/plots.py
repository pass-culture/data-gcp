from pathlib import Path

import matplotlib.pyplot as plt
import mlflow
import pandas as pd
from loguru import logger

from forecast.utils.constants import PRICING_LOWER_BOUND, PRICING_UPPER_BOUND
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

    changepoints = model.changepoints if model.changepoints is not None else []
    for cp in changepoints:
        ax.axvline(cp, color="red", linestyle="--", alpha=0.7)  # type: ignore

    ax.set_title(f"{title}\nNumber of changepoints: {len(changepoints)}")
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


def plot_cv_results(perf: pd.DataFrame, metrics: list, output_dir: str | Path = "cv_plots") -> list[str]:
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
            fig.savefig(str(plot_path))
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
        forecast.yhat_lower.values,
        forecast.yhat_upper.values,
        alpha=0.3,
        label="uncertainty interval",
    )

    ax.set_title(title)
    ax.set_xlabel(x_label)
    ax.set_ylabel(y_label)
    ax.legend()

    fig.tight_layout()

    return fig


def log_diagnostics_plots(
    model: Prophet,
    df_train: pd.DataFrame,
    freq: str,
    backtest_forecast: pd.DataFrame,
) -> dict:
    """Generate and log all diagnostic and evaluation plots to MLflow.

    Args:
        ProphetModelInstance: Instance of the ProphetModel class.
        backtest_forecast: Optional DataFrame with backtest predictions and actuals.
            Must contain: ds, y, yhat, yhat_lower, yhat_upper

    Returns:
        Dictionary containing figure objects for diagnostic plots.
    """

    logger.info("Generating all plots")

    # Generate diagnostic plots
    fig_cp = plot_prophet_changepoints(model, df_train)
    forecast_train = model.predict(df_train)
    changepoints_list = model.changepoints.tolist() if model.changepoints is not None else []
    fig_trend = plot_trend_with_changepoints(forecast_train, changepoints_list)
    fig_components = model.plot_components(forecast_train)
    fig_backtest = plot_forecast_vs_actuals(
        forecast=backtest_forecast,
        freq=freq,
        title="Backtest: Forecast vs Actuals",
        y_label="Pricing €",
    )

    # Log diagnostic figures to MLflow
    logger.info("Logging diagnostic plots to MLflow")
    mlflow.log_figure(fig_cp, "diagnostics/changepoints.png")
    mlflow.log_figure(fig_trend, "diagnostics/trend.png")
    mlflow.log_figure(fig_components, "diagnostics/components.png")
    mlflow.log_figure(fig_backtest, "diagnostics/backtest_forecast.png")

    logger.info("All plots generated and logged successfully")

    return {
        "changepoints": fig_cp,
        "trend": fig_trend,
        "components": fig_components,
    }


def log_future_forecast_plots(
    monthly_forecast: pd.DataFrame,
) -> None:
    """Generate and log future forecast plots to MLflow.

    Args:
        monthly_forecast: DataFrame with future forecast data, must contain:
            ds, yhat, yhat_lower, yhat_upper
    """
    fig_future = plt.figure(figsize=(12, 6))
    plt.plot(
        monthly_forecast.ds,
        monthly_forecast.total_pricing,
    )
    plt.title("Future Monthly Forecast")
    plt.hlines(
        y=PRICING_LOWER_BOUND,
        xmin=monthly_forecast.ds.min(),
        xmax=monthly_forecast.ds.max(),
        colors="orange",
        linestyles="--",
        alpha=0.7,
        label=f"Ligne des {PRICING_LOWER_BOUND / 1e6:.0f} Millions €",
    )
    plt.hlines(
        y=PRICING_UPPER_BOUND,
        xmin=monthly_forecast.ds.min(),
        xmax=monthly_forecast.ds.max(),
        colors="red",
        linestyles="--",
        alpha=0.7,
        label=f"Ligne des {PRICING_UPPER_BOUND / 1e6:.0f} Millions €",
    )

    plt.xlabel("Month")
    plt.ylabel("Total Pricing €")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.legend()

    mlflow.log_figure(fig_future, "forecasts/current_run_monthly_forecast_plot.png")
