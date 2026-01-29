import mlflow
import typer
from loguru import logger

# Import the interface and implementations
from forecast.forecasters.forecast_model import ForecastModel
from forecast.forecasters.prophet_model import ProphetModel
from forecast.utils.mlflow import setup_mlflow


def get_model_class(model_type: str) -> type[ForecastModel]:
    """Factory to return the correct model class."""
    if model_type.lower() == "prophet":
        return ProphetModel
    # Add new models here later:
    else:
        raise ValueError(f"Unknown model type: {model_type}")


def main(
    model_type: str,
    model_name: str,
    train_start_date: str,
    backtest_start_date: str,
    backtest_end_date: str,
    forecast_horizon_date: str,
    run_backtest: bool,
    experiment_name: str,
) -> None:
    """
    Generic main function to train, evaluate, and forecast using any supported model
    class.

    Args:
        model_type: Type of model to use. E.g., 'prophet'.
                    Must be implemented in get_model_class().
        model_name: Name of the model configuration/instance.
        train_start_date: In-sample start date (YYYY-MM-DD).
        backtest_start_date: Out-of-sample start date (YYYY-MM-DD).
        backtest_end_date: Out-of-sample end date (YYYY-MM-DD).
        forecast_horizon_date: Forecast horizon end date (YYYY-MM-DD).
        run_backtest: Whether to evaluate on backtest data.
        experiment_name: MLflow experiment name.
    """
    experiment, run_name = setup_mlflow(experiment_name, model_type, model_name)

    with mlflow.start_run(experiment_id=experiment.experiment_id, run_name=run_name):
        mlflow.set_tag("model_type", model_type)
        mlflow.set_tag("model_name", model_name)

        # 1. Initialize Model
        logger.info(f"Initializing {model_type} model for {model_name}")
        ModelClass = get_model_class(model_type)
        model = ModelClass(model_name)

        # Log config parameters if available
        if model.config is not None and hasattr(model.config, "model_dump"):
            mlflow.log_params(model.config.model_dump())

        # Log config file if available
        if model.config_path and model.config_path.exists():
            mlflow.log_artifact(str(model.config_path), artifact_path="config")
            logger.info(f"Logged config file: {model.config_path}")

        # 2. Prepare Data
        model.prepare_data(train_start_date, backtest_start_date, backtest_end_date)

        # 3. Train
        model.train()

        # 4. Evaluate
        eval_results = model.evaluate()
        mlflow.log_metrics(eval_results)

        if run_backtest:
            backtest_metrics = model.run_backtest()
            mlflow.log_metrics(backtest_metrics)

        # 5. Diagnostics
        ## TODO: Save plots as artifacts

        # 6. Future Forecast
        forecast_df = model.predict(backtest_end_date, forecast_horizon_date)

        # Save forecast to generic artifact
        forecast_file = f"{run_name}_forecast.xlsx"
        forecast_df.to_excel(forecast_file, index=False)
        mlflow.log_artifact(forecast_file, artifact_path="forecasts")
        logger.info(f"Forecast saved to {forecast_file}")

        # 7. Monthly Forecast Aggregation
        try:
            monthly_forecast_df = model.aggregate_to_monthly(forecast_df)
            monthly_forecast_file = f"{run_name}_monthly_forecast.xlsx"
            monthly_forecast_df.to_excel(monthly_forecast_file, index=False)
            mlflow.log_artifact(monthly_forecast_file, artifact_path="forecasts")
            logger.info(f"Monthly Forecast saved to {monthly_forecast_file}")
        except NotImplementedError:
            logger.warning(f"Monthly aggregation not implemented for {model_type}")


if __name__ == "__main__":
    typer.run(main)
