import math

import pandas as pd
from loguru import logger

from src.forecast_engines.prophet.model_config import ModelConfig
from src.forecasters.forecast_model import DataSplit
from src.utils.bigquery import load_table


def validate_data(df: pd.DataFrame, model_config: ModelConfig) -> None:
    """Validate that the dataframe is not empty and contains the required columns
    from the model config.

    Args:
        df: Input dataframe.
        model_config: Training configuration object.

    Raises:
        ValueError: If required columns are missing.
    """
    if df.empty:
        raise ValueError("Input dataframe is empty")

    required_columns = {
        model_config.data_processing.date_column_name,
        model_config.data_processing.target_name,
    }
    required_columns.update(model_config.features.regressors or [])

    missing_columns = required_columns - set(df.columns)
    if missing_columns:
        raise ValueError(
            f"Missing required columns: {missing_columns}. "
            f"Available columns: {df.columns.tolist()}"
        )


def filter_data(
    df: pd.DataFrame,
    backtest_end_date: str,
    model_config: ModelConfig,
) -> pd.DataFrame:
    """Rename time/target columns and filter rows up to the backtest end date.

    Args:
        df: Input dataframe.
        backtest_end_date: End date for backtest data.
        model_config: Training configuration object.

    Returns:
        pd.DataFrame: Preprocessed dataframe.
    """
    date_col = model_config.data_processing.date_column_name
    target_col = model_config.data_processing.target_name

    # Filter by date
    out_of_sample_end = pd.to_datetime(backtest_end_date)
    df = df[pd.to_datetime(df[date_col]) < out_of_sample_end].copy()

    # Rename columns in-place
    df.rename(
        columns={date_col: "ds", target_col: "y"},
        inplace=True,
    )
    df.reset_index(drop=True, inplace=True)
    return df


def add_features(
    df: pd.DataFrame,
    model_config: ModelConfig,
) -> pd.DataFrame:
    """
    Add optional features required for Prophet modelling.
    - Adds pass_culture_months when configured.
    - Adds floor/cap columns when using logistic growth.
        Floor is set to 0, cap to 1.2*max(y).
        which means that the model will never predict negative values
        and will have some headroom above the maximum observed value.

    Args:
        df: Input dataframe.
        model_config: Full configuration object holding feature and growth settings.

    Returns:
        pd.DataFrame: Cleaned dataframe.
    """

    if model_config.features.pass_culture_months:
        months_years = set(model_config.features.pass_culture_months)
        # Avoid intermediate _month_year column by computing membership directly
        df["pass_culture_months"] = (
            pd.to_datetime(df["ds"]).dt.strftime("%m-%Y").isin(months_years)
        )
    if model_config.prophet.growth == "logistic":
        df["floor"] = 0.0
        df["cap"] = df["y"].max() * 1.2
    return df


def select_feature_columns(
    df,
    model_config: ModelConfig,
) -> pd.DataFrame:
    """
    Keep only the relevant columns for Prophet modelling.

    Args:
        df: Dataframe containing features.
        model_config: Full configuration object holding regressor, seasonality,
        and growth settings.
    Returns:
        pd.DataFrame: DataFrame with selected columns.
    """
    cols = ["ds", "y"]
    if model_config.features.regressors:
        cols.extend(model_config.features.regressors)
    if model_config.features.pass_culture_months:
        cols.append("pass_culture_months")
    if model_config.prophet.growth == "logistic":
        cols.extend(["floor", "cap"])
    return df[cols]


def split_train_test_backtest(
    df: pd.DataFrame,
    train_start_date: str,
    backtest_start_date: str,
    model_config: ModelConfig,
) -> DataSplit:
    """
    Split the dataframe into train, test, and backtest sets.

    Args:
        df: Cleaned dataframe with 'ds' and 'y' columns.
        train_start_date: In-sample start date (inclusive).
        backtest_start_date: Out-of-sample start date (inclusive for test, backtest
                        strictly after).
        model_config: Full configuration object with data processing settings including
                     CV flag and train proportion.
    Returns:
        DataSplit: Dataclass containing train, test,
                                and backtest dataframes.
    """
    train_start = pd.to_datetime(train_start_date)
    backtest_start = pd.to_datetime(backtest_start_date)

    # Filter by date range first to reduce data before sorting
    df_in_sample = df[
        (df["ds"] >= train_start) & (df["ds"] <= backtest_start)
    ].sort_values("ds", ignore_index=True)

    # Get backtest data before operating on in_sample
    df_backtest = df[df["ds"] > backtest_start].reset_index(drop=True)

    ## Process In sample split
    cv = model_config.evaluation.cv
    if cv:
        ## override train_prop to 1.0 when using CV
        train_prop = 1.0
        logger.info("CV enabled: using all data for training")
    else:
        train_prop = model_config.data_processing.train_prop
        logger.info(f"CV disabled: using {train_prop} of in-sample data for training")

    train_cutoff = math.ceil(len(df_in_sample) * train_prop)
    df_train = df_in_sample.iloc[:train_cutoff].reset_index(drop=True)
    df_test = df_in_sample.iloc[train_cutoff:].reset_index(drop=True)

    return DataSplit(train=df_train, test=df_test, backtest=df_backtest)


def preprocessing_pipeline(
    train_start_date: str,
    backtest_start_date: str,
    backtest_end_date: str,
    model_config: ModelConfig,
) -> DataSplit:
    """Run preprocessing:
    Steps:
    - Load data from BigQuery.
    - Validate required columns are present.
    - Filter data by date,
    - add configured features,
    - select columns,
    - split into train/test/backtest.

    Args:
        train_start_date: In-sample start date (inclusive).
        backtest_start_date: Out-of-sample start date (inclusive for test, backtest
                            strictly after).
        backtest_end_date: Out-of-sample end date (exclusive).
        model_config: Full configuration object.
    Returns:
        DataSplit: Dataclass containing train, test,
                            and backtest dataframes
    """
    df = load_table(model_config.data_processing.table_name)
    validate_data(df, model_config=model_config)

    # Chain transformations to reduce intermediate copies
    df = filter_data(
        df,
        backtest_end_date=backtest_end_date,
        model_config=model_config,
    )
    df = add_features(df, model_config=model_config)
    df = select_feature_columns(df, model_config=model_config)

    return split_train_test_backtest(
        df=df,
        train_start_date=train_start_date,
        backtest_start_date=backtest_start_date,
        model_config=model_config,
    )
