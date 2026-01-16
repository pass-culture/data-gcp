from pathlib import Path

import pandas as pd
import yaml
from prophet import Prophet

from src.prophet.model import FullConfig

CONFIG_DIR = Path(__file__).parent / "prophet_model_configs"


def load_config():
    with open("config.yaml") as f:
        raw = yaml.safe_load(f)

    cfg = FullConfig(**raw)
    return cfg


def fit_prophet_model(
    df_train: pd.DataFrame,
    prophet_params: dict,
    *,
    regressors: list[str],
    adding_country_holidays: bool,
    add_monthly_seasonality: bool,
    monthly_fourier_order: int,
    pass_culture_months: list[str],
) -> Prophet:
    """Fit a Prophet model with specified parameters and additional features.
    Args:
        df_train: Training dataframe with 'ds' and 'y' columns.
        prophet_params: Dictionary of Prophet parameters.
        regressors: List of regressor column names to add to the model.
        adding_country_holidays: Boolean, whether to add country holidays.
        add_monthly_seasonality: Boolean, whether to add custom monthly seasonality.
        monthly_fourier_order: Fourier order for the monthly seasonality.
        pass_culture_months: List of months for special seasonality events related to
                            pass culture.
    Returns:
        Trained Prophet model.
    """
    model = Prophet(**prophet_params)
    if regressors:
        for reg in regressors:
            model.add_regressor(reg)
    if adding_country_holidays:
        model.add_country_holidays(country_name="FR")
    if add_monthly_seasonality:
        model.add_seasonality(
            name="monthly",
            period=30.5,  # monthly seasonality
            fourier_order=monthly_fourier_order,
        )
    if pass_culture_months:
        model.add_seasonality(
            name="pass_culture",
            period=30.5,  # monthly seasonality
            fourier_order=3,
            condition_name="pass_culture_seasonal_effect",
        )
    model.fit(df_train)
    return model
