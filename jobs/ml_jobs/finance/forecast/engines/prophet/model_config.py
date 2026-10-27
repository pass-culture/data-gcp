from datetime import date
from typing import Literal

from pydantic import BaseModel, Field


class ProphetParams(BaseModel):
    growth: Literal["linear", "logistic", "flat"]
    changepoints: list[date] = Field(
        default_factory=list,
        description="Optional manual changepoint dates; specify as a list of dates",
    )
    changepoint_prior_scale: float = Field(gt=0, description="Flexibility of the trend")
    changepoint_range: float = Field(
        gt=0,
        le=1,
        default=0.9,
        description=(
            "Proportion of history in which to place automatic changepoints. "
            "Ignored by Prophet whenever `changepoints` is non-empty."
        ),
    )
    yearly_seasonality: bool | Literal["auto"]
    weekly_seasonality: bool | Literal["auto"]
    daily_seasonality: bool | Literal["auto"]
    seasonality_mode: Literal["additive", "multiplicative"]
    seasonality_prior_scale: float = Field(gt=0, description="Strength of seasonality")
    interval_width: float = Field(gt=0, lt=1, description="Width of uncertainty intervals")
    scaling: Literal["absmax", "minmax"] | None = None


class FeatureConfig(BaseModel):
    adding_country_holidays: bool
    add_monthly_seasonality: bool
    monthly_fourier_order: int = Field(ge=1, le=20, default=5)
    pass_culture_seasonality: bool = Field(
        default=False,
        description="Enable a June/December conditional seasonality (rolling rule, every year).",
    )
    regressors: list[str] | None = None
    regressor_future_strategy: Literal["hold_last_value", "flat_mean_90d"] = Field(
        default="hold_last_value",
        description="How to fill regressor values over the future forecast horizon.",
    )


class DataProcessingConfig(BaseModel):
    train_prop: float = Field(gt=0.1, lt=1.0, description="Proportion of data for training")
    table_name: str
    date_column_name: str
    target_name: str


class EvaluationConfig(BaseModel):
    cv: bool
    cv_initial: float
    cv_period: float
    cv_horizon: float
    freq: str


class ModelConfig(BaseModel):
    prophet: ProphetParams
    features: FeatureConfig
    data_processing: DataProcessingConfig
    evaluation: EvaluationConfig
