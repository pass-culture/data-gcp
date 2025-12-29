import datetime
import json

import mlflow
from bigquery_utils import load_table
from loguru import logger
from mlflow_utils import connect_remote_mlflow, get_mlflow_experiment
from prophet.serialize import model_to_json
from prophet_evaluate import (
    cross_validate_prophet_model,
    evaluate_prophet_model,
)
from prophet_plots import (
    plot_cv_results,
    plot_prophet_changepoints,
    plot_trend_with_changepoints,
)
from prophet_train import fit_prophet_model, prepare_data


def main(
    *,
    experiment_name: str = "finance_prophet",
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
):
    # -------------------------------------------------
    # MLflow setup
    # -------------------------------------------------
    connect_remote_mlflow()
    experiment = get_mlflow_experiment(experiment_name)

    run_name = f"{model_name}_{datetime.datetime.now():%Y%m%d_%H%M%S}"

    with mlflow.start_run(
        experiment_id=experiment.experiment_id,
        run_name=run_name,
    ):
        # -------------------------------------------------
        # Log high-level tags
        # -------------------------------------------------
        mlflow.set_tags(
            {
                "model_type": "prophet",
                "table_name": table_name,
                "cv": cv,
                "backtest": backtest,
            }
        )

        # -------------------------------------------------
        # Load data
        # -------------------------------------------------
        df = load_table(table_name)

        if cv:
            train_prop = 1.0
            logger.info("CV enabled: using all data for training")

        with open(f"prophet_params/{model_name}.json") as f:
            train_params = json.load(f)

        # -------------------------------------------------
        # Log parameters
        # -------------------------------------------------
        mlflow.log_params(train_params)
        mlflow.log_param("train_prop", train_prop)

        # -------------------------------------------------
        # Prepare data
        # -------------------------------------------------
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
        logger.info(f"Train set size: {len(df_train)}")
        logger.info(f"Test set size: {len(df_test)}")
        logger.info(f"Backtest set size: {len(df_backtest)}")

        # -------------------------------------------------
        # Fit model
        # -------------------------------------------------
        model = fit_prophet_model(df_train, **train_params)

        # -------------------------------------------------
        # Save & log Prophet model
        # -------------------------------------------------
        model_file = f"{model_name}.json"
        with open(model_file, "w") as f:
            f.write(model_to_json(model))

        mlflow.log_artifact(model_file, artifact_path="model")

        # -------------------------------------------------
        # In-sample / CV evaluation
        # -------------------------------------------------
        if cv:
            cv_metrics, cv_perf = cross_validate_prophet_model(
                model,
                cv_initial,
                cv_period,
                cv_horizon,
                metrics=("mae", "rmse", "mape"),
            )

            # Log CV metrics
            for k, v in cv_metrics.items():
                mlflow.log_metric(f"cv_{k}", v)

            # CV plots
            cv_figs = plot_cv_results(cv_perf, cv_metrics)
            for metric, fig in cv_figs.items():
                mlflow.log_figure(fig, f"plots/cv_{metric}.png")

        else:
            test_forecast_df, test_metrics = evaluate_prophet_model(model, df_test)

            for k, v in test_metrics.items():
                mlflow.log_metric(f"test_{k}", v)

            # Log test forecast dataframe
            test_xlsx = "test_forecast.xlsx"
            test_forecast_df.to_excel(test_xlsx, index=False)
            mlflow.log_artifact(test_xlsx, artifact_path="data")

        # -------------------------------------------------
        # Backtest evaluation
        # -------------------------------------------------
        if backtest:
            backtest_forecast_df, backtest_metrics = evaluate_prophet_model(
                model, df_backtest
            )

            for k, v in backtest_metrics.items():
                mlflow.log_metric(f"backtest_{k}", v)

            backtest_xlsx = "backtest_forecast.xlsx"
            backtest_forecast_df.to_excel(backtest_xlsx, index=False)
            mlflow.log_artifact(backtest_xlsx, artifact_path="data")

        # -------------------------------------------------
        # Diagnostic plots
        # -------------------------------------------------
        fig_cp = plot_prophet_changepoints(model, df_train)
        mlflow.log_figure(fig_cp, "plots/changepoints_train.png")

        forecast_train = model.predict(df_train)
        fig_trend = plot_trend_with_changepoints(forecast_train, model.changepoints)
        mlflow.log_figure(fig_trend, "plots/trend_changepoints.png")

        fig_components = model.plot_components(forecast_train)
        mlflow.log_figure(fig_components, "plots/components.png")

        logger.info("MLflow run completed successfully.")
