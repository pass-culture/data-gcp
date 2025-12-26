import pandas as pd

from src.bigquery_utils import load_table
from src.constants import (
    IS_start_date,
    OOS_end_date,
    OOS_start_date,
)
from src.prophet_utils import run_prophet_evaluation

df_daily = load_table("daily_pricing")
df_daily.pricing_day = pd.to_datetime(df_daily.pricing_day)
df_daily = df_daily[df_daily.pricing_day < OOS_end_date].reset_index(
    drop=True
)  ## Limit to data before OOS end date


target_name = "total_pricing"
datetime_name = "pricing_day"
confidence_level = 0.95
train_prop = 0.7
freq = "D"  ## Daily frequency
changepoints = [
    "2022-12-15",
    "2023-07-20",
    "2023-10-20",
    "2024-03-15",
    "2024-07-3",
    "2024-12-20",
    "2025-06-25",
]
prophet_params = {
    "changepoints": changepoints,
    "yearly_seasonality": True,
    "weekly_seasonality": True,
    "daily_seasonality": False,
    "seasonality_prior_scale": 5,  # defaults to 10, lower=smoother seasonal,
    # good range(1-10)
    "scaling": "absmax",
    "interval_width": confidence_level,
    "growth": "logistic",  ## If logistic growth provide cap and floor
    # "changepoint_prior_scale": 10, ## smaller=smoother trend, larger=more flexible aka
    # overfitting
    # "n_changepoints": 20
}

# Specify pass culture months as MM-YYYY strings
pass_culture_months = (
    None  # ["06-2023", "06-2024", "06-2025", "12-2023", "12-2024", "12-2025"]
)

cv = True
# if you want cross validation take all in sample in cross validation so no test set
if cv:
    train_prop = 1.0


def train_prophet():
    results = run_prophet_evaluation(
        df_daily,
        IS_start_date,
        OOS_start_date,
        target_name,
        datetime_name,
        prophet_params,
        train_prop=train_prop,
        freq=freq,
        cross_val=cv,
        plot_train=False,
        plot_changepoints=True,
        plot_cv=False,
        verbose=True,
        adding_country_holidays=True,
        add_monthly_seasonality=True,
        monthly_fourier_order=1,
        pass_culture_months=pass_culture_months,
        backtest=True,
        cap=df_daily["total_pricing"].max()
        * 1.1,  ## If logistic growth provide cap and floor
        floor=df_daily["total_pricing"].min(),
        xlabel="days",
    )
    return results["model"]
