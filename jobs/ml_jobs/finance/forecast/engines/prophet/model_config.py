from datetime import date
from typing import Literal

from pydantic import BaseModel, Field


class ProphetParams(BaseModel):
    growth: Literal["linear", "logistic", "flat"]
    changepoints: list[date] = Field(default_factory=list)
    changepoint_prior_scale: float = Field(gt=0, description="Flexibility of the trend")
    yearly_seasonality: bool | Literal["auto"]
    weekly_seasonality: bool | Literal["auto"]
    daily_seasonality: bool | Literal["auto"]
    seasonality_mode: Literal["additive", "multiplicative"]
    seasonality_prior_scale: float = Field(gt=0, description="Strength of seasonality")
    interval_width: float = Field(
        gt=0, lt=1, description="Width of uncertainty intervals"
    )
    scaling: Literal["absmax", "minmax"] | None = None


class FeatureConfig(BaseModel):
    adding_country_holidays: bool
    add_monthly_seasonality: bool
    monthly_fourier_order: int = Field(ge=1, le=20, default=5)
    pass_culture_months: list[str] | None = None
    regressors: list[str] | None = None


class DataProcessingConfig(BaseModel):
    train_prop: float = Field(
        gt=0.1, lt=1.0, description="Proportion of data for training"
    )
    table_name: str
    date_column_name: str
    target_name: str


class EvaluationConfig(BaseModel):
    cv: bool
    cv_initial: str
    cv_period: str
    cv_horizon: str
    freq: str


class ModelConfig(BaseModel):
    prophet: ProphetParams
    features: FeatureConfig
    data_processing: DataProcessingConfig
    evaluation: EvaluationConfig
