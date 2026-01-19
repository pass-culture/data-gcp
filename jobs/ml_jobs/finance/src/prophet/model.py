from pathlib import Path

import pandas as pd
import yaml
from loguru import logger
from prophet.serialize import model_to_json

from src.interfaces import ForecastModel
from src.prophet.evaluate import backtest_pipeline, evaluation_pipeline
from src.prophet.forecast import generate_future_forecast
from src.prophet.model_config import ModelConfig
from src.prophet.plots import log_diagnostic_plots
from src.prophet.preprocessing import preprocessing_pipeline
from src.prophet.train import fit_prophet_model


class ProphetModel(ForecastModel):
    CONFIG_DIR = Path(__file__).parent / "prophet_model_configs"

    def __init__(self, model_name: str):
        super().__init__(model_name=model_name)
        self.config = self._load_config()

    def _load_config(self) -> ModelConfig:
        config_path = self.CONFIG_DIR / f"{self.model_name}.yaml"
        if not config_path.exists():
            raise FileNotFoundError(
                f"Configuration file not found: {config_path}. "
                f"Available: {list(self.CONFIG_DIR.glob('*.yaml'))}"
            )

        with open(config_path) as f:
            raw = yaml.safe_load(f)
        return ModelConfig(**raw)

    def prepare_data(
        self, train_start_date: str, backtest_start_date: str, backtest_end_date: str
    ):
        logger.info("Running Prophet preprocessing pipeline...")
        self.data_split = preprocessing_pipeline(
            train_start_date=train_start_date,
            backtest_start_date=backtest_start_date,
            backtest_end_date=backtest_end_date,
            model_config=self.config,
        )
        logger.info(
            f"Train: {len(self.data_split.train)}, "
            f"Test: {len(self.data_split.test)}, "
            f"Backtest: {len(self.data_split.backtest)}"
        )
        return self.data_split

    def train(self):
        logger.info(f"Training Prophet model: {self.model_name}")
        self.model = fit_prophet_model(self.data_split.train, self.config)
        return self.model

    def save_model(self) -> str:
        model_file = f"{self.model_name}.json"
        with open(model_file, "w") as f:
            f.write(model_to_json(self.model))
        logger.info(f"Model saved locally: {model_file}")
        return model_file

    def evaluate(self) -> dict:
        return evaluation_pipeline(
            model=self.model,
            df_test=self.data_split.test,
            model_config=self.config,
        )

    def run_backtest(self) -> dict:
        return backtest_pipeline(df_backtest=self.data_split.backtest, model=self.model)

    def get_diagnostics(self) -> dict:
        plots = log_diagnostic_plots(self.model, self.data_split.train)
        logger.info("Diagnostic plots generated")
        return plots

    def predict(self, start_date: str, end_date: str) -> pd.DataFrame:
        logger.info(f"Generating forecast from {start_date} to {end_date}")
        return generate_future_forecast(
            model=self.model,
            train_test_backtest_split=self.data_split,
            forecast_start_date=start_date,
            forecast_end_date=end_date,
            model_config=self.config,
        )
