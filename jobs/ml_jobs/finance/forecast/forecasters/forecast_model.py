from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

import pandas as pd


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
        self, train_start_date: str, backtest_start_date: str, backtest_end_date: str
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
    def run_backtest(self) -> dict[str, Any]:
        """Run backtest evaluation and return metrics."""
        pass

    @abstractmethod
    def predict(self, start_date: str, end_date: str) -> pd.DataFrame:
        """Generate forecasts for a specific horizon."""
        pass

    @abstractmethod
    def aggregate_to_monthly(self, forecast_df: pd.DataFrame) -> pd.DataFrame:
        """Aggregate forecast to monthly level."""
        pass
