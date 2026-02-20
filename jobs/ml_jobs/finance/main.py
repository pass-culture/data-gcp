import mlflow
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
        help="""In-sample start date (YYYY-MM-DD).
        If you are using a prophet config with changepoints,
        ensure this date is before the first changepoint date.""",
    ),
    backtest_start_date: str = typer.Option(
        ...,
        help="""Out-of-sample start date (YYYY-MM-DD).
         If you are using a prophet config with changepoints,
         ensure this date is after the last changepoint date.""",
    ),
    backtest_end_date: str = typer.Option(
        ..., help="Out-of-sample end date (YYYY-MM-DD)."
    ),
    forecast_horizon_date: str = typer.Option(
        ..., help="Forecast horizon end date (YYYY-MM-DD)."
    ),
    experiment_name: str = typer.Option(..., help="MLflow experiment name."),
    dataset: str = typer.Option(
        ..., help="BigQuery dataset containing training data and forecast results."
    ),
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
        experiment_name: MLflow experiment name.
        dataset: BigQuery dataset containing the training data and forecast results.
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

        # 5. Future Forecast
        forecast_df = model.predict(backtest_end_date, forecast_horizon_date)

        # Save forecast to generic artifact
        forecast_file = f"{run_name}_forecast.xlsx"
        forecast_df.to_excel(forecast_file, index=False)
        mlflow.log_artifact(forecast_file, artifact_path="forecasts")
        logger.info(f"Forecast saved to {forecast_file}")

        # 6. Monthly Forecast Aggregation
        try:
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
        except NotImplementedError:
            logger.warning(f"Monthly aggregation not implemented for {model_type}")
        except Exception as e:
            logger.error(f"Failed to log to BigQuery: {e}")


if __name__ == "__main__":
    typer.run(main)
