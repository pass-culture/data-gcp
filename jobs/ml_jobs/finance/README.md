# Folder Documentation

This folder contains code and resources for predicting monthly pricing forecasts for the DAF (Pass Culture Finance Team). The primary forecasting tool used is the Prophet model, which enables accurate time series predictions. Currently, two types of models are supported within this folder: a daily model and a weekly model. These models allow for flexible forecasting granularity, supporting both daily and weekly data inputs to generate monthly pricing predictions.

## Project Structure

```
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

The main entry point for running Prophet-based forecasts is `main_prophet.py`. You can configure the run by specifying parameters such as the experiment name, model name, table name, and date/target columns.

Example usage (with Typer CLI):

```bash
uv run python main_prophet.py --model-name daily_pricing --table-name <your_table> --date-column-name <date_col> --target-name <target_col>
```

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
