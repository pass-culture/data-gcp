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

## Modelling choices

The pipeline trains **two complementary [Prophet](https://facebook.github.io/prophet/)
models** on historical pricing and uses them jointly in the report:

- **`daily_pricing`** — fits the daily pricing series (`daily_pricing` table). It is
  the most granular view and captures short-term dynamics.
- **`weekly_pricing`** — fits the weekly aggregated series (`weekly_pricing` table).
  Aggregation smooths daily noise, which makes the trend and seasonality more stable
  and the medium-term forecast more robust.

Keeping both lets the Finance team cross-check a noisy-but-reactive forecast against
a smoother one instead of relying on a single point of view.

### Why Prophet

Prophet decomposes the series into **trend + seasonality + holidays** in an additive,
fully interpretable way. It handles missing days, outliers and irregular history well,
exposes native **uncertainty intervals** (used for the 95 % bands), and lets us inject
**business knowledge** (changepoints, holidays, custom seasonalities) — all of which
matter more here than squeezing out marginal accuracy with a black-box model.

### Key hyperparameters

| Choice | `daily_pricing` | `weekly_pricing` | Rationale |
| --- | --- | --- | --- |
| `growth` | `logistic` | `linear` | Daily pricing is bounded, so a logistic curve with a `floor = 0` (no negative pricing) and a `cap = 1.2 × max(y)` (headroom above the historical max) keeps forecasts realistic. The smoother weekly series is modelled with a plain linear trend. |
| `changepoints` | manual list | manual list | Trend breaks are pinned to **known business inflection points** (product/budget changes) rather than letting Prophet place them automatically. Changepoints outside the training window are dropped automatically at run time (`_filter_changepoints`). |
| `changepoint_prior_scale` | `0.05` | `0.05` | Moderately low → a **flexible but not jumpy** trend; limits overfitting to local noise. |
| `yearly_seasonality` | ✅ | ✅ | Strong yearly cycle in cultural spending. |
| `weekly_seasonality` | ✅ | ✅ | Intra-week patterns in daily pricing. |
| `daily_seasonality` | ❌ | ✅ | The daily series has one point per day (no intraday signal to learn), so it is disabled there. |
| custom **monthly** seasonality | ✅ (`period = 30.5`, `fourier_order = 5`) | ✅ | Captures the monthly billing rhythm that Prophet's defaults do not model. |
| **`pass_culture` conditional seasonality** | — | ✅ (June & December) | An extra seasonality that is **only active on specific months** (e.g. June/December campaign peaks) via Prophet's `condition_name`, so these recurring spikes don't distort the rest of the year. |
| French holidays | ✅ | ✅ | `add_country_holidays("FR")` to absorb public-holiday effects. |
| `seasonality_mode` | `additive` | `additive` | Seasonal amplitude is roughly constant in level, not proportional to the trend. |
| `seasonality_prior_scale` | `5` | `1` | Allows a bit more seasonal flexibility on the noisier daily series. |
| `interval_width` | `0.95` | `0.95` | 95 % uncertainty bands shown to Finance. |
| `scaling` | `absmax` | `absmax` | Robust input scaling. |

### Training, evaluation & backtest

- **Sliding window** — all dates are derived from `execution_date`: the training set
  ends at `execution_date − backtest_days`, the backtest spans the last `backtest_days`,
  and the forecast runs up to `execution_date + forecast_days`.
- **Cross-validation** (`cv: true`) uses rolling-origin evaluation sized as fractions
  of the training history (`cv_initial = 60 %`, `cv_period = 15 %`, `cv_horizon = 30 %`).
  When CV is disabled, an 80/20 train/test split is used instead (`train_prop = 0.8`).
- **Backtest** — the model is scored on the held-out recent window; metrics
  (**MAE, RMSE, MAPE** and bias) are computed on the **monthly aggregation**, which is
  the granularity the Finance team actually consumes.

See the per-model YAML files for the exact values.

## Reporting (compte rendu DAF)

After both models (daily & weekly) have been trained, `reporting/generate_report.py`
produces an automatic French report for the Finance team. It combines the two
MLflow runs with observed pricing and past runs from BigQuery to generate:

- a multi-sheet Excel workbook (`compte_rendu_pricing_<year>.xlsx`): monthly
  forecasts vs observed pricing over the report year **and the whole next year**
  (the model's operational horizon is ignored here), quarterly/annual totals,
  backtest detail & metrics (with the months considered, a metric glossary and the
  over/under prediction trend), metrics evolution across runs and run-to-run
  forecast comparison (both with an embedded chart), and the model configuration
  summary;
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
`forecast/forecasters/configs/prophet/` (`daily_pricing.yaml`, `weekly_pricing.yaml`).

## Development

- **Linting**: `make ruff_check`
- **Testing**: `uv run pytest`
