# Folder Documentation

This folder contains code and resources for predicting monthly pricing forecasts for the DAF (Pass Culture Finance Team). The primary forecasting tool used is the Prophet model, which enables accurate time series predictions. Currently, two types of models are supported within this folder: a daily model and a weekly model. These models allow for flexible forecasting granularity, supporting both daily and weekly data inputs to generate monthly pricing predictions.

## Key Features

- **Prophet-based Time Series Forecasting**: Leverages Facebook's Prophet for robust forecasting
- **Multiple Frequency Support**: Daily and weekly granularity models
- **MLflow Integration**: Automatic experiment tracking and model versioning
- **Cross-Validation**: Built-in CV support for model evaluation
- **BigQuery Integration**: Direct data loading from BigQuery tables
- **Comprehensive Diagnostics**: Automated generation of diagnostic plots and metrics

## Project Structure

```text
DAF_pricing_forecast/
├── Makefile
├── README.md
├── main_prophet.py
├── pyproject.toml
├── src/
│   ├── __init__.py
│   ├── prophet/
│   │   ├── __init__.py
│   │   ├── prophet_evaluate.py
│   │   ├── prophet_plots.py
│   │   ├── prophet_predict.py
│   │   ├── prophet_train.py
│   │   └── prophet_model_configs/
│   │       ├── daily_pricing.json
│   │       └── weekly_pricing.json
│   └── utils/
│       ├── __init__.py
│       ├── bigquery.py
│       ├── constants.py
│       └── mlflow.py
├── tests/
│   ├── __init__.py
│   └── hello_test.py
```

## Makefile Commands

- `make setup`        : Create a virtual environment using `uv`.
- `make install`      : Install all dependencies as specified in `pyproject.toml` using `uv`.
- `make ruff_fix`     : Run Ruff linter and auto-fix issues, then format code.
- `make ruff_check`   : Run Ruff linter and check formatting (does not auto-fix).

## Running the Pipeline

The main entry point for running Prophet-based forecasts is `main_prophet.py`. You can configure the run by specifying parameters such as model name and date ranges.

Example usage (with Typer CLI):

```bash
uv run python main_prophet.py \
  --model-name daily_pricing \
  --train-start-date 2022-01-01 \
  --backtest-start-date 2024-01-01 \
  --backtest-end-date 2024-12-31 \
  --forecast-horizon-date 2025-12-31 \
  --run-backtest
```

### Configuration Files

Model configurations are stored in `src/prophet/prophet_model_configs/` as YAML files. Each configuration includes:

- **prophet**: Prophet model hyperparameters (growth, seasonality, changepoints)
- **features**: Feature engineering settings (holidays, regressors, seasonality)
- **data_proc**: Data processing parameters (table name, column names, train/test split)
- **evaluation**: Evaluation settings (CV parameters, frequency)

Available configurations:

- `daily_pricing.yaml`: Daily granularity with logistic growth
- `weekly_pricing.yaml`: Weekly granularity with linear growth and Pass Culture specific seasonality

## Dependencies

All dependencies are managed in `pyproject.toml` and installed via `uv`. Key packages include:

- prophet
- mlflow
- pandas
- numpy
- matplotlib
- scikit-learn
- loguru
- google-cloud-aiplatform
- google-cloud-secret-manager
- tqdm
- typer
- ruff (for linting)

## Testing

Basic tests can be found in the `tests/` directory. To run tests (if using pytest):

```bash
uv run pytest
```

## Notes

- Model configuration files are in `src/prophet/prophet_model_configs/`.
- Utility functions for BigQuery, MLflow, and constants are in `src/utils/`.
- Linting and formatting are handled by Ruff (see Makefile commands).
