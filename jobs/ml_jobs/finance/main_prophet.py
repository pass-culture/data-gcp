from pathlib import Path

import mlflow
import typer
import yaml
from loguru import logger
from prophet.serialize import model_to_json

from src.prophet.evaluate import evaluation_pipeline
from src.prophet.forecast import generate_future_forecast
from src.prophet.model_config import FullConfig
from src.prophet.plots import log_diagnostic_plots
from src.prophet.preprocessing import preprocessing_pipeline
from src.prophet.train import fit_prophet_model
from src.utils.constants import EXPERIMENT_NAME
from src.utils.mlflow import setup_mlflow

CONFIG_DIR = Path(__file__).parent / "src" / "prophet" / "prophet_model_configs"


def load_config(model_name: str) -> FullConfig:
    """Load configuration from YAML file.

    Args:
        model_name: Name of the model configuration file (without .yaml extension).

    Returns:
        FullConfig: Parsed configuration object.

    Raises:
        FileNotFoundError: If the configuration file doesn't exist.
        ValueError: If the configuration is invalid.
    """
    config_path = CONFIG_DIR / f"{model_name}.yaml"
    if not config_path.exists():
        raise FileNotFoundError(
            f"Configuration file not found: {config_path}. "
            f"Available configs: {list(CONFIG_DIR.glob('*.yaml'))}"
        )

    with open(config_path) as f:
        raw = yaml.safe_load(f)

    cfg = FullConfig(**raw)
    return cfg


def main(
    model_name: str,
    train_start_date: str,
    backtest_start_date: str,
    backtest_end_date: str,
    forecast_horizon_date: str,
    *,
    run_backtest: bool = True,
) -> None:
    """Main function to train, evaluate, and log a Prophet model using MLflow.

    Args:
        model_name: Name of the model (used for saving/loading parameters).
        train_start_date: In-sample start date (YYYY-MM-DD).
        backtest_start_date: Out-of-sample start date (YYYY-MM-DD).
        backtest_end_date: Out-of-sample end date (YYYY-MM-DD).
        forecast_horizon_date: Forecast horizon end date (YYYY-MM-DD).
        run_backtest: Whether to evaluate on backtest data.

    Returns: None
    """
    experiment, run_name = setup_mlflow(EXPERIMENT_NAME, model_name)
    with mlflow.start_run(
        experiment_id=experiment.experiment_id,
        run_name=run_name,
    ):
        mlflow.set_tag("frequency", model_name)
        full_config = load_config(model_name)
        mlflow.log_params(full_config.model_dump())

        # Prepare datasets
        train_test_backtest_split = preprocessing_pipeline(
            train_start_date=train_start_date,
            backtest_start_date=backtest_start_date,
            backtest_end_date=backtest_end_date,
            full_config=full_config,
        )
        logger.info(
            f"""Train set size: {len(train_test_backtest_split.train)},
            Test set size: {len(train_test_backtest_split.test)},
            Backtest set size: {len(train_test_backtest_split.backtest)}"""
        )

        # Train model and save it as artifact
        model = fit_prophet_model(train_test_backtest_split.train, full_config)

        model_file = f"{model_name}.json"
        with open(model_file, "w") as f:
            f.write(model_to_json(model))
        mlflow.log_artifact(model_file, artifact_path="model")
        logger.info(f"Model saved as artifact: {model_file}")

        # Evaluate and log results
        eval_results = evaluation_pipeline(
            model,
            train_test_backtest_split.test,
            train_test_backtest_split.backtest,
            full_config.data_proc,
            full_config.evaluation,
            run_backtest=run_backtest,
        )

        # Log metrics to MLflow based on evaluation mode
        if "cv" in eval_results:
            cv_metrics = eval_results["cv"]["metrics"]
            mlflow.log_metrics(
                {f"cv/{metric}": value for metric, value in cv_metrics.items()}
            )
            logger.info(f"Logged CV metrics: {cv_metrics}")
        else:
            test_metrics = eval_results["test"]["metrics"]
            mlflow.log_metrics(
                {f"test/{metric}": value for metric, value in test_metrics.items()}
            )
            logger.info(f"Logged test metrics: {test_metrics}")

        if "backtest" in eval_results:
            backtest_metrics = eval_results["backtest"]["metrics"]
            mlflow.log_metrics(
                {
                    f"backtest/{metric}": value
                    for metric, value in backtest_metrics.items()
                }
            )
            logger.info(f"Logged backtest metrics: {backtest_metrics}")

        # Generate and log diagnostic plots
        diagnostic_plots = log_diagnostic_plots(model, train_test_backtest_split.train)
        mlflow.log_figure(
            diagnostic_plots["changepoints"], "plots/changepoints_train.png"
        )
        mlflow.log_figure(diagnostic_plots["trend"], "plots/trend_changepoints.png")
        mlflow.log_figure(diagnostic_plots["components"], "plots/components.png")
        logger.info("Diagnostic plots logged to MLflow")

        # Generate future forecast from backtest end date to forecast horizon
        logger.info(
            f"Generating forecast from {backtest_end_date} to {forecast_horizon_date}"
        )
        forecast_future = generate_future_forecast(
            model=model,
            train_test_backtest_split=train_test_backtest_split,
            forecast_start_date=backtest_end_date,
            forecast_end_date=forecast_horizon_date,
            full_config=full_config,
        )
        logger.info(f"Future forecast generated: {len(forecast_future)} periods")

        # Save forecast to Excel
        forecast_xlsx = f"{run_name}_forecast.xlsx"
        forecast_future.to_excel(forecast_xlsx, index=False)
        mlflow.log_artifact(forecast_xlsx, artifact_path="forecasts")
        logger.info("Future forecast saved and logged to MLflow")

        # Cleanup temporary files
        import os

        try:
            os.remove(model_file)
            os.remove(forecast_xlsx)
            logger.info("Temporary files cleaned up")
        except OSError as e:
            logger.warning(f"Failed to cleanup temporary files: {e}")


if __name__ == "__main__":
    typer.run(main)
