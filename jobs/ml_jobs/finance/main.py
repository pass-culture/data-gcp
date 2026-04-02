from datetime import datetime, timedelta

import mlflow
import pandas as pd
import typer
from loguru import logger

# Import the interface and implementations
from forecast.forecasters.forecast_model import ForecastModel
from forecast.forecasters.prophet_model import ProphetModel
from forecast.utils.bigquery import save_forecast_gbq
from forecast.utils.mlflow import setup_mlflow


def get_model_class(model_type: str) -> type[ForecastModel]:
    """Factory to return the correct model class."""
    if model_type.lower() == "prophet":
        return ProphetModel
    # Add new models here later:
    else:
        raise ValueError(f"Unknown model type: {model_type}")


def main(
    model_type: str = typer.Option(..., help="Type of model to use. E.g., 'prophet'."),
    model_name: str = typer.Option(
        ..., help="Name of the model configuration/instance."
    ),
    train_start_date: str = typer.Option(
        ...,
        help="In-sample start date (YYYY-MM-DD). Must be before first changepoint.",
    ),
    execution_date: str = typer.Option(
        ...,
        help="DAG execution date (YYYY-MM-DD). Used to compute sliding window dates.",
    ),
    backtest_days: int = typer.Option(
        120,
        help="Number of days for backtest period (counted back from execution_date).",
    ),
    forecast_days: int = typer.Option(
        365,
        help="Number of days for forecast horizon",
    ),
    experiment_name: str = typer.Option(..., help="MLflow experiment name."),
    dataset: str = typer.Option(
        ..., help="BigQuery dataset containing training data and forecast results."
    ),
) -> None:
    """
    Train, evaluate, and forecast using a sliding window approach.

    Dates are computed from execution_date:
    - backtest_start = execution_date - backtest_days
    - backtest_end = execution_date - 1 day
    - forecast_horizon = execution_date + forecast_days

    Changepoints outside the training period are automatically filtered.
    """
    # Compute dates from execution_date
    exec_date = datetime.strptime(execution_date, "%Y-%m-%d")
    backtest_start_date = (exec_date - timedelta(days=backtest_days)).strftime(
        "%Y-%m-%d"
    )
    backtest_end_date = (exec_date - timedelta(days=1)).strftime("%Y-%m-%d")
    forecast_horizon_date = (exec_date + timedelta(days=forecast_days)).strftime(
        "%Y-%m-%d"
    )

    experiment, run_name = setup_mlflow(experiment_name, model_type, model_name)
    with mlflow.start_run(experiment_id=experiment.experiment_id, run_name=run_name):
        mlflow.set_tag("model_type", model_type)
        mlflow.set_tag("model_name", model_name)

        # Log sliding window parameters
        mlflow.log_params(
            {
                "train_start_date": train_start_date,
                "backtest_start_date": backtest_start_date,
                "backtest_end_date": backtest_end_date,
                "forecast_horizon_date": forecast_horizon_date,
            }
        )

        # 1. Initialize Model
        logger.info(f"Initializing {model_type} model for {model_name}")
        ModelClass = get_model_class(model_type)
        model = ModelClass(model_name)

        # Log config file if available
        if model.config_path and model.config_path.exists():
            mlflow.log_artifact(str(model.config_path), artifact_path="config")

        # 2. Prepare Data
        model.prepare_data(
            dataset, train_start_date, backtest_start_date, backtest_end_date
        )

        # 3. Train
        model.train()

        # 4. Evaluate on test/CV and backtest data
        eval_results = model.evaluate()
        mlflow.log_metrics(eval_results)

        backtest_metrics = model.run_backtest()
        mlflow.log_metrics(backtest_metrics)

        # 5. Future Forecast - start from 1st of month after last data point
        last_data_date = model.data_split.backtest["ds"].max()
        forecast_start = (last_data_date + pd.offsets.MonthBegin(1)).strftime(
            "%Y-%m-%d"
        )
        logger.info(f"Last data: {last_data_date}, forecast starts: {forecast_start}")

        forecast_df = model.predict(forecast_start, forecast_horizon_date)

        # Save forecast to generic artifact
        forecast_file = f"{run_name}_forecast.xlsx"
        forecast_df.to_excel(forecast_file, index=False)
        mlflow.log_artifact(forecast_file, artifact_path="forecasts")
        logger.info(f"Forecast saved to {forecast_file}")

        # 6. Monthly Forecast Aggregation
        monthly_forecast_df = model.aggregate_to_monthly(forecast_df)
        monthly_forecast_file = f"{run_name}_monthly_forecast.xlsx"
        monthly_forecast_df.to_excel(monthly_forecast_file, index=False)
        mlflow.log_artifact(monthly_forecast_file, artifact_path="forecasts")
        logger.info(f"Monthly Forecast saved to {monthly_forecast_file}")

        # 7. Log to BigQuery
        logger.info("Logging monthly forecast to BigQuery...")
        save_forecast_gbq(
            df=monthly_forecast_df,
            run_id=mlflow.active_run().info.run_id,
            experiment_name=experiment_name,
            run_name=run_name,
            model_name=model_name,
            model_type=model_type,
            table_name="monthly_forecasts",
            dataset=dataset,
        )
        logger.info("Monthly forecast logged to BigQuery successfully")


if __name__ == "__main__":
    typer.run(main)
