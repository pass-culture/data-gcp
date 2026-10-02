"""Generate and deliver the French finance "compte rendu" after a forecast run.

This CLI is meant to run right after both Prophet models (daily & weekly) have
been trained. It takes the two MLflow run ids, pulls their artifacts, enriches
them with observed pricing and past runs from BigQuery, and produces a single
multi-sheet Excel workbook, PNG charts and a French markdown summary.

Deliverables are logged back to the MLflow runs (``report/`` artifact path),
optionally uploaded to GCS, and a one-line French summary is printed as the
last stdout line so Airflow can forward it to Slack via XCom.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd
import typer
import yaml
from loguru import logger
from mlflow.tracking import MlflowClient

from forecast.utils.bigquery import get_past_runs, load_query
from forecast.utils.constants import GCP_PROJECT_ID
from forecast.utils.mlflow import connect_remote_mlflow
from reporting import report_builder as rb

app = typer.Typer(add_completion=False)


def _to_month_start(series: pd.Series) -> pd.Series:
    """Normalise a date/datetime series to the first day of its month (Timestamp)."""
    return pd.to_datetime(series).dt.to_period("M").dt.to_timestamp()


def _aggregate_backtest_monthly(backtest: pd.DataFrame, freq: str) -> pd.DataFrame:
    """Aggregate the backtest forecast to monthly sums, keeping only complete months.

    Partial boundary months (fewer observed periods than a full month) are dropped
    because their tiny denominators make MAPE explode and distort the metrics. If
    every month is partial, all months are kept as a fallback.

    Args:
        backtest: Backtest forecast at the model frequency (columns ds, y, yhat).
        freq: Model frequency (``"D"`` for daily, ``"W-*"`` for weekly).

    Returns:
        DataFrame with columns month (Timestamp), y, yhat.
    """
    df = backtest.copy()
    df["month"] = _to_month_start(df["ds"])
    grouped = df.groupby("month")
    monthly = grouped[["y", "yhat"]].sum(min_count=1)
    monthly["n_periods"] = grouped.size()
    monthly = monthly.reset_index()

    min_periods = 4 if freq.upper().startswith("W") else 28
    complete = monthly[monthly["n_periods"] >= min_periods]
    if not complete.empty:
        monthly = complete
    return monthly[["month", "y", "yhat"]].reset_index(drop=True)


def _load_run_data(client: MlflowClient, run_id: str, label: str) -> rb.ModelReportData:
    """Load params, config and forecast/backtest artifacts for a single run."""
    run = client.get_run(run_id)
    params = dict(run.data.params)
    tags = dict(run.data.tags)
    model_name = tags.get("model_name", params.get("model_name", label))
    model_type = tags.get("model_type", "prophet")
    run_name = tags.get("mlflow.runName", run_id)

    with TemporaryDirectory() as tmp:
        local_root = Path(client.download_artifacts(run_id, "", tmp))

        config = _read_config(local_root, model_name)

        full_monthly = _read_single(local_root, "*_full_year_monthly_forecast.xlsx")
        full_monthly = full_monthly.rename(columns={"ds": "month"})
        full_monthly["month"] = _to_month_start(full_monthly["month"])

        backtest = _read_single(local_root, "*_backtest_forecast.xlsx")
        freq = str(config.get("evaluation", {}).get("freq", "D"))
        backtest_monthly = _aggregate_backtest_monthly(backtest, freq)

    return rb.ModelReportData(
        label=label,
        model_name=model_name,
        model_type=model_type,
        run_id=run_id,
        run_name=run_name,
        params=params,
        config=config,
        full_monthly=full_monthly,
        backtest_monthly=backtest_monthly,
    )


def _read_single(root: Path, pattern: str) -> pd.DataFrame:
    matches = sorted(root.rglob(pattern))
    if not matches:
        raise FileNotFoundError(f"No artifact matching {pattern!r} under {root}")
    return pd.read_excel(matches[0])


def _read_config(root: Path, model_name: str) -> dict:
    matches = sorted(root.rglob("*.yaml")) + sorted(root.rglob("*.yml"))
    for match in matches:
        if model_name in match.stem or match.parent.name == "config":
            with open(match) as f:
                return yaml.safe_load(f) or {}
    logger.warning(f"No config artifact found for {model_name}; config sheet will be partial.")
    return {}


def _load_real_pricing(dataset: str) -> pd.DataFrame:
    """Load observed monthly pricing from the daily_pricing table."""
    query = f"""
        SELECT
            DATE_TRUNC(pricing_day, MONTH) AS month,
            SUM(total_pricing) AS real_total
        FROM `{GCP_PROJECT_ID}.{dataset}.daily_pricing`
        GROUP BY month
        ORDER BY month
    """
    df = load_query(query)
    df["month"] = _to_month_start(df["month"])
    df["real_total"] = pd.to_numeric(df["real_total"], errors="coerce")
    return df


def _load_metrics_evolution(
    client: MlflowClient,
    experiment_id: str,
    models: list[rb.ModelReportData],
    n_past_runs: int,
) -> list[dict]:
    """Collect backtest metrics across the last N runs of each model."""
    records: list[dict] = []
    for data in models:
        runs = client.search_runs(
            experiment_ids=[experiment_id],
            filter_string=f"tags.model_name = '{data.model_name}'",
            order_by=["attributes.start_time DESC"],
            max_results=n_past_runs,
        )
        for run in runs:
            metrics = run.data.metrics
            if not {"MAE", "RMSE", "MAPE"} <= set(metrics):
                continue
            records.append(
                {
                    "model": data.label,
                    "date": datetime.fromtimestamp(run.info.start_time / 1000),
                    "run_name": run.data.tags.get("mlflow.runName", run.info.run_id),
                    "MAE": metrics["MAE"],
                    "RMSE": metrics["RMSE"],
                    "MAPE": metrics["MAPE"],
                }
            )
    return records


def _upload_to_gcs(local_dir: Path, gcs_output_path: str) -> None:
    """Upload every file in ``local_dir`` to a gs:// destination."""
    from google.cloud import storage

    if not gcs_output_path.startswith("gs://"):
        raise ValueError(f"gcs_output_path must start with gs:// (got {gcs_output_path!r})")
    bucket_name, _, prefix = gcs_output_path[len("gs://") :].partition("/")
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    for file_path in sorted(local_dir.iterdir()):
        if not file_path.is_file():
            continue
        blob_name = f"{prefix.rstrip('/')}/{file_path.name}" if prefix else file_path.name
        bucket.blob(blob_name).upload_from_filename(str(file_path))
        logger.info(f"Uploaded {file_path.name} to gs://{bucket_name}/{blob_name}")


@app.command()
def main(
    daily_run_id: str = typer.Option(..., help="MLflow run id of the daily Prophet model."),
    weekly_run_id: str = typer.Option(..., help="MLflow run id of the weekly Prophet model."),
    dataset: str = typer.Option(..., help="BigQuery dataset holding daily_pricing and monthly_forecasts."),
    report_year: int = typer.Option(..., help="Reference year of the report (e.g. 2026)."),
    n_past_runs: int = typer.Option(6, help="Number of past runs to include in comparisons."),
    gcs_output_path: str = typer.Option("", help="Optional gs:// destination for the report files."),
    output_dir: str = typer.Option("finance_report", help="Local output directory."),
) -> None:
    """Build the finance report from the two model runs and deliver it."""
    next_year = report_year + 1
    connect_remote_mlflow()
    client = MlflowClient()

    logger.info("Loading run data from MLflow...")
    daily = _load_run_data(client, daily_run_id, "Prophet Daily")
    weekly = _load_run_data(client, weekly_run_id, "Prophet Weekly")
    experiment_id = client.get_run(daily_run_id).info.experiment_id

    logger.info("Loading observed pricing and past runs from BigQuery...")
    real_df = _load_real_pricing(dataset)
    past_monthly_forecasts = get_past_runs(n_past_runs, dataset)
    evolution_records = _load_metrics_evolution(client, experiment_id, [daily, weekly], n_past_runs)

    logger.info("Building report tables and charts...")
    tidy = rb.build_tidy_monthly(daily, weekly, real_df)
    monthly_sheet = rb.build_monthly_sheet(tidy)
    quarterly = rb.build_quarterly_annual(tidy, years=[report_year, next_year])
    backtest_detail = rb.build_backtest_detail(daily, weekly)
    backtest_metrics_sheet, backtest_metrics = rb.build_backtest_metrics(daily, weekly)
    metrics_evolution = rb.build_metrics_evolution(evolution_records)
    runs_comparison = rb.build_runs_comparison(past_monthly_forecasts)
    config_summary = rb.build_config_summary(daily, weekly)

    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    # Charts are generated first so they can be embedded into the workbook sheets.
    rb.plot_monthly(tidy, out)
    rb.plot_quarterly(quarterly, out)
    evolution_png = rb.plot_metrics_evolution(evolution_records, out)
    comparison_png = rb.plot_runs_comparison(past_monthly_forecasts, out)

    backtest_info = rb.build_backtest_info(daily, weekly)
    backtest_glossary = rb.backtest_metric_glossary()

    sheets = {
        "Prévisions mensuelles": monthly_sheet,
        "Totaux trim. & annuels": quarterly,
        "Backtest détail": backtest_detail,
        "Backtest métriques": {
            "blocks": [
                ("Métriques de backtest (agrégation mensuelle)", backtest_metrics_sheet),
                ("Période et mois considérés", backtest_info),
                ("Définition des métriques", backtest_glossary),
            ],
        },
        "Évolution métriques": {
            "blocks": [("Évolution des métriques backtest par run", metrics_evolution)],
            "images": [(evolution_png, None)],
        },
        "Comparaison runs": {
            "blocks": [("Prévisions mensuelles par run", runs_comparison)],
            "images": [(comparison_png, None)],
        },
        "Configuration": config_summary,
    }
    workbook_path = out / f"compte_rendu_pricing_{report_year}.xlsx"
    rb.write_workbook(workbook_path, sheets)
    logger.info(f"Workbook written to {workbook_path}")

    markdown, one_line = rb.build_summary(
        report_year=report_year,
        next_year=next_year,
        quarterly=quarterly,
        backtest_metrics=backtest_metrics,
        daily=daily,
        weekly=weekly,
        n_past_runs=n_past_runs,
    )
    summary_path = out / "synthese.md"
    summary_path.write_text(markdown, encoding="utf-8")
    logger.info(f"Summary written to {summary_path}")

    # Attach the report to both model runs so it is reachable from either.
    for run_id in (daily_run_id, weekly_run_id):
        client.log_artifacts(run_id, str(out), artifact_path="report")
    logger.info("Report logged to MLflow under report/ for both runs")

    if gcs_output_path:
        _upload_to_gcs(out, gcs_output_path)

    # Final stdout line -> captured by Airflow SSHGCEOperator XCom (key="result").
    print(one_line)


if __name__ == "__main__":
    app()
