from datetime import date

from pydantic import BaseModel


class ProphetParams(BaseModel):
    growth: str  #'linear', 'logistic' or 'flat'
    changepoints: list[date]
    changepoint_prior_scale: float
    yearly_seasonality: bool
    weekly_seasonality: bool
    daily_seasonality: bool
    seasonality_mode: str
    seasonality_prior_scale: float
    interval_width: float
    scaling: str  # among "absmax" or "minmax"


class FeatureConfig(BaseModel):
    adding_country_holidays: bool
    add_monthly_seasonality: bool
    monthly_fourier_order: int
    pass_culture_months: list[str] | None
    regressors: list[str] | None


class DataProcConfig(BaseModel):
    cv: bool
    train_prop: float
    table_name: str
    date_column_name: str
    target_name: str


# Alias for backward compatibility
DataProcessConfig = DataProcConfig


class EvaluationConfig(BaseModel):
    cv_initial: str
    cv_period: str
    cv_horizon: str
    freq: str


class FullConfig(BaseModel):
    prophet: ProphetParams
    features: FeatureConfig
    data_proc: DataProcessConfig
    evaluation: EvaluationConfig
