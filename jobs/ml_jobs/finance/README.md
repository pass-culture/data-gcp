# DAF Pricing Forecast

Forecast pipeline for the Finance team (DAF) to predict monthly pricing metrics using **Prophet** on **BigQuery** data.

## Features

- **Automated Forecasting**: Fit Prophet models with custom seasonality and holidays.
- **BigQuery Integration**: Direct data loading from production tables.
- **MLflow Tracking**: Logs model parameters, metrics, and artifact reports.
- **Dual Output**: Generates both granular daily forecasts and aggregated monthly reports.

## Installation

This project uses `uv` for dependency management.

```bash
make setup    # Initialize virtual environment
make install  # Install dependencies
```

## Usage

Run the pipeline using `main.py`.

```bash
uv run main.py --model-type 'prophet' --model-name 'daily_pricing' --train-start-date '2022-01-01' --execution-date '2026-01-01' --backtest-days 90 --forecast-days 365  --experiment-name "finance_pricing_forecast_v0_dev" --dataset "ml_finance_dev"
```

### Arguments

| Argument | Description | Example |
|----------|-------------|---------|
| `model_type` | Model implementation to use | `prophet` |
| `model_name` | Configuration file name (see `configs/`) | `daily_pricing` |
| `train_start_date` | Start date for training data | `2022-01-01` |
| `execution_date` | Run Start date  | `2026-01-01` |
| `backtest_days` | number of days to backtest on | 90 |
| `forecast_days` | number of days to forecast | 365 |
| `experiment_name` | MLflow experiment name | `finance_pricing_forecast_v0_dev` |
| `dataset`| Bigquery dataset name | `ml_finance_dev` |

## Project Structure

```text
main.py                # Pipeline entrypoint
forecast/
├── forecasters/       # High-level model wrappers (Abstract Base Class & specific implementations)
│                      # Handles config loading, data flow, and orchestrates the engine.
└── engines/           # Low-level core logic for each model type (e.g., Prophet)
    └── prophet/       # specific training, prediction, plotting, and preprocessing functions.
reporting/             # Automated French "compte rendu" for the Finance team (DAF)
│                      # Builds a multi-sheet Excel workbook, PNG charts and a markdown summary.
tests/                 # Unit tests
```

## Reporting (compte rendu DAF)

After both models (daily & weekly) have been trained, `reporting/generate_report.py`
produces an automatic French report for the Finance team. It combines the two
MLflow runs with observed pricing and past runs from BigQuery to generate:

- a multi-sheet Excel workbook (`compte_rendu_pricing_<year>.xlsx`): monthly
  forecasts vs observed pricing, quarterly/annual totals for the current and next
  year, backtest detail & metrics (over/under prediction), metrics evolution across
  runs, run-to-run forecast comparison, and the model configuration summary;
- PNG charts;
- a French markdown summary (`synthese.md`).

Deliverables are logged to MLflow under the `report/` artifact path of both runs,
optionally uploaded to GCS, and a one-line summary is printed on the last stdout
line (consumed by Airflow/Slack).

```bash
uv run python -m reporting.generate_report \
    --daily-run-id '<daily_mlflow_run_id>' \
    --weekly-run-id '<weekly_mlflow_run_id>' \
    --dataset 'ml_finance_prod' \
    --report-year 2026 \
    --n-past-runs 6 \
    --gcs-output-path 'gs://<bucket>/.../report'
```

## Configuration

Model hyperparameters and feature settings are defined in YAML files located in:
`forecast/engines/prophet/configs/`

## Development

- **Linting**: `make ruff_check`
- **Testing**: `uv run pytest`
