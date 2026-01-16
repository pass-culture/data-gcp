import datetime
import math
from dataclasses import dataclass

import pandas as pd
from loguru import logger

from src.prophet.model_config import FullConfig


@dataclass
class TrainTestBacktestSplit:
    train: pd.DataFrame
    test: pd.DataFrame
    backtest: pd.DataFrame


def filter_data(
    df: pd.DataFrame,
    out_of_sample_end_date: str,
    full_config: FullConfig,
) -> pd.DataFrame:
    """
    Rename time/target columns and filter rows up to the out-of-sample end date.
    Args:
        df: Input dataframe.
        out_of_sample_end_date: End date for out-of-sample data.
        full_config: Training configuration object.
    Returns:
        pd.DataFrame: Preprocessed dataframe.
    """
    df[full_config.training.date_column_name] = pd.to_datetime(
        df[full_config.training.date_column_name]
    )
    df = df.rename(
        columns={
            full_config.training.date_column_name: "ds",
            full_config.training.target_name: "y",
        }
    )
    out_of_sample_end = pd.to_datetime(out_of_sample_end_date)
    df = df[df["ds"] < out_of_sample_end].reset_index(drop=True)
    return df


def add_features(
    df: pd.DataFrame,
    full_config: FullConfig,
) -> pd.DataFrame:
    """
    Add optional features required for Prophet modelling.
    - Adds pass_culture_seasonal_effect when configured.
    - Adds floor/cap columns when using logistic growth.

    Args:
        df: Input dataframe.
        full_config: Full configuration object holding feature and growth settings.

    Returns:
        pd.DataFrame: Cleaned dataframe.
    """

    if full_config.features.pass_culture_months:
        months_years = set(full_config.features.pass_culture_months)
        df["_month_year"] = df["ds"].dt.strftime("%m-%Y")
        df["pass_culture_seasonal_effect"] = df["_month_year"].isin(months_years)
        df.drop(columns=["_month_year"], inplace=True)
    if full_config.prophet.growth == "logistic":
        df["floor"] = 0.0
        df["cap"] = df["y"].max() * 1.2
    return df


def select_feature_columns(
    df,
    full_config: FullConfig,
) -> pd.DataFrame:
    """
    Keep only the relevant columns for Prophet modelling.

    Args:
        df: Dataframe containing features.
        full_config: Full configuration object holding regressor, seasonality,
        and growth settings.
    Returns:
        pd.DataFrame: DataFrame with selected columns.
    """
    cols = ["ds", "y"]
    if full_config.features.regressors:
        cols.extend(full_config.features.regressors)
    if full_config.features.pass_culture_months:
        cols.append("pass_culture_seasonal_effect")
    if full_config.prophet.growth == "logistic":
        cols.extend(["floor", "cap"])
    return df[cols]


def split_train_test_backtest(
    df: pd.DataFrame,
    train_start_date: str,
    backtest_start_date: str,
    full_config: FullConfig,
) -> TrainTestBacktestSplit:
    """
    Split the dataframe into train, test, and backtest sets.

    Args:
        df: Cleaned dataframe with 'ds' and 'y' columns.
        train_start_date: In-sample start date (inclusive).
        backtest_start_date: Out-of-sample start date (inclusive for test, backtest
                        strictly after).
        full_config: Training configuration object with CV flag and train proportion.
    Returns:
        TrainTestBacktestSplit: Dataclass containing train, test,
                                and backtest dataframes.
    """
    train_start = datetime.datetime.fromisoformat(train_start_date)
    backtest_start = datetime.datetime.fromisoformat(backtest_start_date)

    # Ensure chronological order before splitting
    df = df.sort_values("ds").reset_index(drop=True)

    ## Process In sample split
    cv = full_config.training.cv
    if cv:
        train_prop = 1.0
        logger.info("CV enabled: using all data for training")
    else:
        train_prop = full_config.training.train_prop
        logger.info(f"CV disabled: using {train_prop} of in-sample data for training")
    df_in_sample = df[(df["ds"] >= train_start) & (df["ds"] <= backtest_start)]
    train_cutoff = math.ceil(df_in_sample.shape[0] * train_prop)
    df_train = df_in_sample.iloc[:train_cutoff].copy().reset_index(drop=True)
    df_test = df_in_sample.iloc[train_cutoff:].copy().reset_index(drop=True)

    ## Process Backtest split
    df_backtest = df.loc[df["ds"] > backtest_start].reset_index(drop=True)

    return TrainTestBacktestSplit(train=df_train, test=df_test, backtest=df_backtest)


def preprocessing_pipeline(
    df: pd.DataFrame,
    train_start_date: str,
    backtest_start_date: str,
    backtest_end_date: str,
    full_config: FullConfig,
) -> TrainTestBacktestSplit:
    """Run preprocessing: filter, add configured features, select columns, then split.
    Args:
        df: Input dataframe.
        train_start_date: In-sample start date (inclusive).
        backtest_start_date: Out-of-sample start date (inclusive for test, backtest
                            strictly after).
        backtest_end_date: Out-of-sample end date (exclusive).
        full_config: Full configuration object.
    Returns:
        TrainTestBacktestSplit: Dataclass containing train, test,
                            and backtest dataframes
    """
    df_clean = filter_data(
        df,
        out_of_sample_end_date=backtest_end_date,
        full_config=full_config,
    )
    df_features = add_features(
        df_clean,
        full_config=full_config,
    )
    df = select_feature_columns(
        df_features,
        full_config=full_config,
    )
    train_test_backtest_split = split_train_test_backtest(
        df=df,
        train_start_date=train_start_date,
        backtest_start_date=backtest_start_date,
        full_config=full_config,
    )
    return train_test_backtest_split
