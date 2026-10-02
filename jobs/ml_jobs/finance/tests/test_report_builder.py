"""Unit tests for the finance report builder (pure, offline helpers)."""

import pandas as pd
import pytest

from reporting import report_builder as rb


def _model(label, name, growth, freq, full_base, backtest_bias):
    months = pd.date_range("2026-01-01", periods=18, freq="MS")
    full_monthly = pd.DataFrame({"month": months, "total_pricing": [full_base + i * 1e5 for i in range(len(months))]})
    bt_months = pd.date_range("2026-02-01", periods=4, freq="MS")
    y = [6e6, 6.2e6, 6.4e6, 6.6e6]
    backtest_monthly = pd.DataFrame({"month": bt_months, "y": y, "yhat": [v + backtest_bias for v in y]})
    return rb.ModelReportData(
        label=label,
        model_name=name,
        model_type="prophet",
        run_id=f"{name}-run",
        run_name=f"prophet_{name}_20260601",
        params={
            "train_start_date": "2022-01-01",
            "backtest_start_date": "2026-02-01",
            "backtest_end_date": "2026-05-31",
            "forecast_horizon_date": "2027-05-31",
        },
        config={
            "prophet": {"growth": growth, "changepoints": [], "interval_width": 0.95},
            "evaluation": {"freq": freq},
        },
        full_monthly=full_monthly,
        backtest_monthly=backtest_monthly,
    )


@pytest.fixture()
def daily():
    return _model("Prophet Daily", "daily_pricing", "logistic", "D", 6e6, 3e5)


@pytest.fixture()
def weekly():
    return _model("Prophet Weekly", "weekly_pricing", "linear", "W-MON", 5.8e6, -4e5)


@pytest.fixture()
def real_df():
    return pd.DataFrame(
        {
            "month": pd.date_range("2026-01-01", periods=5, freq="MS"),
            "real_total": [5.9e6, 6.0e6, 6.1e6, 6.2e6, 6.3e6],
        }
    )


def test_tidy_monthly_merges_all_sources(daily, weekly, real_df):
    tidy = rb.build_tidy_monthly(daily, weekly, real_df)
    assert list(tidy.columns) == ["month", "daily", "weekly", "real"]
    # Real is only known for the first 5 months.
    assert tidy["real"].notna().sum() == 5
    assert len(tidy) == 18


def test_quarterly_annual_has_totals(daily, weekly, real_df):
    tidy = rb.build_tidy_monthly(daily, weekly, real_df)
    q = rb.build_quarterly_annual(tidy, years=[2026, 2027])
    periods = set(q["Période"])
    assert "Total 2026" in periods
    assert "Total 2027" in periods
    assert "T1 2026" in periods
    # Annual daily total equals the sum of the 12 monthly daily values for 2026.
    expected = tidy[tidy["month"].dt.year == 2026]["daily"].sum().round(0)
    got = q.loc[q["Période"] == "Total 2026", rb.DAILY_COL].iloc[0]
    assert got == expected


def test_backtest_metrics_detect_over_and_under_prediction(daily, weekly):
    _, metrics = rb.build_backtest_metrics(daily, weekly)
    assert metrics["Prophet Daily"]["bias_eur"] > 0  # +300k each month -> overprediction
    assert metrics["Prophet Weekly"]["bias_eur"] < 0  # -400k each month -> underprediction


def test_backtest_metrics_table_labels_trend(daily, weekly):
    table, _ = rb.build_backtest_metrics(daily, weekly)
    trends = dict(zip(table["Modèle"], table["Tendance"], strict=True))
    assert trends["Prophet Daily"] == "Surestimation"
    assert trends["Prophet Weekly"] == "Sous-estimation"


def test_config_summary_contains_both_models(daily, weekly):
    cfg = rb.build_config_summary(daily, weekly)
    assert daily.label in cfg.columns
    assert weekly.label in cfg.columns
    assert "Fréquence" in set(cfg["Paramètre"])


def test_summary_builds_markdown_and_one_line(daily, weekly, real_df):
    tidy = rb.build_tidy_monthly(daily, weekly, real_df)
    q = rb.build_quarterly_annual(tidy, years=[2026, 2027])
    _, metrics = rb.build_backtest_metrics(daily, weekly)
    md, one_line = rb.build_summary(
        report_year=2026,
        next_year=2027,
        quarterly=q,
        backtest_metrics=metrics,
        daily=daily,
        weekly=weekly,
        n_past_runs=6,
    )
    assert "Compte rendu" in md
    assert "surestime" in md  # daily overpredicts
    assert "Compte rendu pricing 2026" in one_line


def test_format_eur_french_style():
    assert rb.format_eur(1234567) == "1 234 567 €"
    assert rb.format_eur(None) == "n/a"


def test_write_excel_creates_all_sheets(tmp_path, daily, weekly, real_df):
    tidy = rb.build_tidy_monthly(daily, weekly, real_df)
    sheets = {
        "Prévisions mensuelles": rb.build_monthly_sheet(tidy),
        "Configuration": rb.build_config_summary(daily, weekly),
    }
    path = tmp_path / "report.xlsx"
    rb.write_excel(path, sheets)
    assert path.exists()
    loaded = pd.read_excel(path, sheet_name=None)
    assert set(loaded.keys()) == {"Prévisions mensuelles", "Configuration"}
