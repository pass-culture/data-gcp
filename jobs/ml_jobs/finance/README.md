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
uv run main.py \
    'prophet' \
    'daily_pricing' \
    '2022-01-01' \
    '2025-01-01' \
    '2025-12-01' \
    "2026-12-30" \
    True \
    "finance_pricing_forecast_v0_PROD"
```

### Arguments

| Argument | Description | Example |
|----------|-------------|---------|
| `model_type` | Model implementation to use | `prophet` |
| `model_name` | Configuration file name (see `configs/`) | `daily_pricing` |
| `train_start_date` | Start date for training data | `2022-01-01` |
| `backtest_start_date` | Start date for evaluation | `2025-01-01` |
| `backtest_end_date` | End of evaluation / Start of forecast | `2025-12-01` |
| `prediction_full_horizon` | End date for the final forecast | `2026-12-30` |
| `run_backtest` | Run historical evaluation | `True` |
| `experiment_name` | MLflow experiment name | `finance_prod` |

## Project Structure

```text
main.py                # Pipeline entrypoint
forecast/
├── forecasters/       # High-level model wrappers (Abstract Base Class & specific implementations)
│                      # Handles config loading, data flow, and orchestrates the engine.
└── engines/           # Low-level core logic for each model type (e.g., Prophet)
    └── prophet/       # specific training, prediction, plotting, and preprocessing functions.
tests/                 # Unit tests
```

## Configuration

Model hyperparameters and feature settings are defined in YAML files located in:
`forecast/engines/prophet/configs/`

## Development

- **Linting**: `make ruff_check`
- **Testing**: `uv run pytest`
