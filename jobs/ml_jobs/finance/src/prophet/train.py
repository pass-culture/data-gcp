import pandas as pd
from loguru import logger
from prophet import Prophet

from src.prophet.model_config import FullConfig


def fit_prophet_model(
    df_train: pd.DataFrame,
    full_config: FullConfig,
) -> Prophet:
    """Fit a Prophet model with specified parameters and additional features.
    Args:
        df_train: Training dataframe with 'ds' and 'y' columns.
        full_config: Full configuration object holding parameters and feature settings.
    Returns:
        Trained Prophet model.
    """
    prophet_params = full_config.prophet
    features_config = full_config.features

    logger.info(f"Initializing Prophet model with params={prophet_params.model_dump()}")
    model = Prophet(**prophet_params.model_dump())

    if features_config.regressors:
        logger.info(
            f"""Adding {len(features_config.regressors)} regressor(s):
            {features_config.regressors}"""
        )
        for reg in features_config.regressors:
            if reg not in df_train.columns:
                raise ValueError(
                    f"Regressor '{reg}' not found in training data columns"
                )
            model.add_regressor(reg)

    if features_config.adding_country_holidays:
        logger.info("Adding French country holidays")
        model.add_country_holidays(country_name="FR")

    if features_config.add_monthly_seasonality:
        logger.info(
            f"""Adding monthly seasonality with fourier_order=
            {features_config.monthly_fourier_order}"""
        )
        model.add_seasonality(
            name="monthly",
            period=30.5,
            fourier_order=features_config.monthly_fourier_order,
        )
    if features_config.pass_culture_months:
        logger.info(
            f"""Adding pass_culture conditional seasonality for
            {features_config.pass_culture_months} months"""
        )
        if "pass_culture_months" not in df_train.columns:
            raise ValueError(
                "pass_culture_months column not found in training data. "
                "Ensure preprocessing added this feature."
            )
        model.add_seasonality(
            name="pass_culture",
            period=30.5,
            fourier_order=features_config.monthly_fourier_order,
            condition_name="pass_culture_months",
        )

    logger.info(f"Fitting Prophet model on {len(df_train)} training observations")
    model.fit(df_train)
    logger.info("Model fitting completed successfully")
    return model
