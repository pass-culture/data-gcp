import pandas as pd
from loguru import logger

from forecast.engines.prophet.model_config import ModelConfig
from prophet import Prophet


def fit_prophet_model(
    df_train: pd.DataFrame,
    model_config: ModelConfig,
) -> Prophet:
    """Fit a Prophet model with specified parameters and additional features.
    Args:
        df_train: Training dataframe with 'ds' and 'y' columns.
        model_config: Full configuration object holding parameters and feature settings.
    Returns:
        Trained Prophet model.
    """
    prophet_params_dict = model_config.prophet.model_dump()
    features_config = model_config.features

    logger.info(f"Initializing Prophet model with params={prophet_params_dict}")
    model = Prophet(**prophet_params_dict)

    if features_config.regressors:
        logger.info(f"""Adding regressor(s): {features_config.regressors}""")
        for reg in features_config.regressors:
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
