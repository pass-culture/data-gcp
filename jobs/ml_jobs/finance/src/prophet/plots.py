from pathlib import Path

import matplotlib.pyplot as plt
import mlflow
import pandas as pd
from loguru import logger
from prophet import Prophet

from src.prophet.predict import create_full_prediction_dataframe


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
    perf: pd.DataFrame, metrics: list, output_dir: str = "cv_plots"
) -> list[str]:
    """
    Plots cross-validation results for specified metrics and saves figures.

    Args:
        perf: DataFrame with cross-validation performance metrics.
        metrics: List of metric names to plot.
        output_dir: Directory to save the plots.

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


def log_diagnostic_plots(model: Prophet, df_train: pd.DataFrame) -> None:
    """Generate and log diagnostic plots for the Prophet model to MLflow.
    Args:
        model: Trained Prophet model.
        df_train: DataFrame used for training with 'ds' and 'y' columns.
    Returns: None
    """
    fig_cp = plot_prophet_changepoints(model, df_train)
    mlflow.log_figure(fig_cp, "plots/changepoints_train.png")
    forecast_train = model.predict(df_train)
    fig_trend = plot_trend_with_changepoints(forecast_train, model.changepoints)
    mlflow.log_figure(fig_trend, "plots/trend_changepoints.png")
    fig_components = model.plot_components(forecast_train)
    mlflow.log_figure(fig_components, "plots/components.png")
    logger.info("MLflow run completed successfully.")


def log_full_forecast(
    model: Prophet,
    df: pd.DataFrame,
    train_params: dict,
    backtest_end_date: str,
    target_name: str,
) -> None:
    """Generate and log full forecast to MLflow.
    Args:
        model: Trained Prophet model.
        df: Original DataFrame with historical data.
        train_params: Dictionary with training parameters including 'freq'.
        backtest_end_date: String, end date of out-of-sample period (YYYY-MM-DD).
        target_name: String, name of the target variable column.
    Returns: None
    """
    df_forecast_full = create_full_prediction_dataframe(
        start_date=backtest_end_date,
        end_date="2026-12-31",
        freq=train_params.get("freq"),
        cap=df[target_name].max() * 1.2,
        floor=0.0,
    )
    full_forecast = model.predict(df_forecast_full)
    full_forecast_xlsx = "full_forecast.xlsx"
    full_forecast.to_excel(full_forecast_xlsx, index=False)
    mlflow.log_artifact(full_forecast_xlsx, artifact_path="data")
