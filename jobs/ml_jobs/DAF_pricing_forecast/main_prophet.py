import mlflow
import typer
from loguru import logger
from prophet.serialize import model_to_json

from src.prophet.evaluate import evaluate_and_log
from src.prophet.plots import log_diagnostic_plots, log_full_forecast
from src.prophet.train import (
    fit_prophet_model,
    load_data_and_params,
    prepare_data,
)
from src.utils.mlflow import setup_mlflow


def main(
    *,
    experiment_name: str = "pricing_forecast_prophet",
    model_name: str,
    table_name: str,
    date_column_name: str,
    target_name: str,
    train_prop: float = 0.7,
    IS_start_date: str,
    OOS_start_date: str,
    OOS_end_date: str,
    cv: bool = True,
    cv_initial: str = "730 days",
    cv_period: str = "180 days",
    cv_horizon: str = "365 days",
    backtest: bool = True,
) -> None:
    """Main function to train, evaluate, and log a Prophet model using MLflow.

    Args:
        experiment_name: Name of the MLflow experiment.
        model_name: Name of the model (used for saving/loading parameters).
        table_name: BigQuery table name to load data from.
        date_column_name: Name of the date column in the data.
        target_name: Name of the target variable column.
        train_prop: Proportion of data to use for training (if not CV).
        IS_start_date: In-sample start date (YYYY-MM-DD).
        OOS_start_date: Out-of-sample start date (YYYY-MM-DD).
        OOS_end_date: Out-of-sample end date (YYYY-MM-DD).
        cv: Whether to perform cross-validation.
        cv_initial: Initial training period for CV.
        cv_period: Period between cutoffs for CV.
        cv_horizon: Forecast horizon for CV.
        backtest: Whether to evaluate on backtest data.

    Returns: None
    """
    experiment, run_name = setup_mlflow(experiment_name, model_name)
    with mlflow.start_run(
        experiment_id=experiment.experiment_id,
        run_name=run_name,
    ):
        # Load data and parameters
        df, train_params, train_prop = load_data_and_params(
            table_name, model_name, cv, train_prop
        )
        mlflow.log_params(train_params)
        mlflow.log_param("train_prop", train_prop)

        # Prepare datasets
        df_train, df_test, df_backtest = prepare_data(
            df=df,
            IS_start_date=IS_start_date,
            OOS_start_date=OOS_start_date,
            OOS_end_date=OOS_end_date,
            target_name=target_name,
            date_column_name=date_column_name,
            train_prop=train_prop,
            pass_culture_months=train_params.get("pass_culture_months"),
            growth=train_params.get("growth"),
            regressors=train_params.get("regressors"),
        )
        logger.info(
            f"""Train set size: {len(df_train)}, Test set size: {len(df_test)},
            Backtest set size: {len(df_backtest)}"""
        )

        # Train model and log
        model = fit_prophet_model(df_train, **train_params)
        model_file = f"{model_name}.json"
        with open(model_file, "w") as f:
            f.write(model_to_json(model))
        mlflow.log_artifact(model_file, artifact_path="model")

        # Evaluate and log results
        evaluate_and_log(
            model,
            df_test,
            df_backtest,
            cv,
            cv_initial,
            cv_period,
            cv_horizon,
            backtest,
        )
        log_diagnostic_plots(model, df_train)
        log_full_forecast(model, df, train_params, OOS_end_date, target_name)


if __name__ == "__main__":
    typer.run(main)
