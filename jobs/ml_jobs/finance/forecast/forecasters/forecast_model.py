from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

import matplotlib.pyplot as plt
import pandas as pd

from forecast.utils.constants import PRICING_LOWER_BOUND, PRICING_UPPER_BOUND


@dataclass
class DataSplit:
    train: pd.DataFrame
    test: pd.DataFrame
    backtest: pd.DataFrame


class ForecastModel(ABC):
    """Abstract base class for all forecasting models."""

    def __init__(self, model_name: str):
        self.model_name = model_name
        self.config_path = None
        self.config = self._load_config()
        self.model = None
        self.data_split = None

    @abstractmethod
    def _load_config(self):
        """Load model-specific configuration."""
        pass

    @abstractmethod
    def prepare_data(
        self,
        dataset: str,
        train_start_date: str,
        backtest_start_date: str,
        backtest_end_date: str,
    ) -> DataSplit:
        """Prepare train/test/backtest splits and store in self.data_split."""
        pass

    @abstractmethod
    def train(self) -> Any:
        """Train the model using prepared data."""
        pass

    @abstractmethod
    def evaluate(self) -> dict[str, Any]:
        """
        Evaluate the model.
        Returns a dictionary of metrics {'MAE': value, 'RMSE': value, ...}.
        """
        pass

    @abstractmethod
    def run_backtest(self) -> tuple[dict[str, Any], pd.DataFrame]:
        """Run backtest evaluation and return metrics and forecast data.

        Returns:
            Tuple containing:
                - Dictionary with backtest metrics
                - DataFrame with backtest forecast
        """
        pass

    @abstractmethod
    def predict(self, start_date: str, end_date: str) -> pd.DataFrame:
        """Generate forecasts for a specific horizon."""
        pass

    @abstractmethod
    def aggregate_to_monthly(self, forecast_df: pd.DataFrame) -> pd.DataFrame:
        """Aggregate forecast to monthly level."""
        pass

    @abstractmethod
    def log_plots(self, backtest_forecast: pd.DataFrame, monthly_forecast: pd.DataFrame) -> None:
        """Log all relevant plots to MLflow.

        Args:
            backtest_forecast: DataFrame with backtest predictions and actuals
                            (full datafarame with yhat, yhat_lower, yhat_upper, etc).
            monthly_forecast: DataFrame with monthly forecast data.
                                Exactly two columns: ds, total_pricing

        """
        pass

    def plot_last_runs_forecasts(self, past_monthly_forecasts):
        """Plot the monthly forecasts of past runs for comparison.
        Args:
            past_monthly_forecasts: DataFrame with past runs forecasts, containing at least
                                    'run_name', 'forecast_date', 'prediction' columns.
        Returns:
            Matplotlib figure object with the comparison plot.
        """

        ## Plot all past runs monthly forecasts
        fig, ax = plt.subplots(figsize=(12, 6))
        for run_name, g in past_monthly_forecasts.sort_values("forecast_date").groupby("run_name"):
            ax.plot(
                g["forecast_date"],
                g["prediction"],
                marker="o",
                linewidth=2,
                label=run_name,
            )
        # Add horizontal lines at PRICING_LOWER_BOUND and PRICING_UPPER_BOUND,
        # considered as important thresholds for the monthly pricing
        ax.axhline(
            y=PRICING_LOWER_BOUND, color="orange", linestyle="--", label=f"{PRICING_LOWER_BOUND / 1e6:.0f}M threshold"
        )
        ax.axhline(
            y=PRICING_UPPER_BOUND, color="red", linestyle="--", label=f"{PRICING_UPPER_BOUND / 1e6:.0f}M threshold"
        )
        ax.set_xlabel("Forecast date")
        ax.set_ylabel("Prediction")
        ax.legend(title="run_name", bbox_to_anchor=(1.02, 1), loc="upper left")
        ax.grid(True, alpha=0.3)
        fig.autofmt_xdate()
        fig.tight_layout()
        return fig

    def compute_average_forecast(
        self,
        past_monthly_forecasts: pd.DataFrame,
    ) -> pd.DataFrame:
        """Compute average forecast of past runs for comparison.
        Args:
            past_monthly_forecasts: DataFrame with past runs forecasts, containing at least
                                    'run_name', 'forecast_date', 'prediction' columns.
        Returns:
            DataFrame with average forecast across past runs, containing 'forecast_date'
            and 'prediction' columns.
        """
        ## Compute average forecast of past runs
        avg_forecast_df = past_monthly_forecasts.groupby("forecast_date")["prediction"].mean().reset_index()

        return avg_forecast_df
