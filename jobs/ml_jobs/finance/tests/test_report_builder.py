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


def test_aggregate_backtest_monthly_drops_partial_months():
    from reporting.generate_report import _aggregate_backtest_monthly

    bt = pd.DataFrame(
        {
            "ds": (
                list(pd.date_range("2026-02-27", periods=2, freq="D"))  # partial Feb -> dropped
                + list(pd.date_range("2026-03-01", periods=31, freq="D"))
                + list(pd.date_range("2026-04-01", periods=30, freq="D"))
            ),
        }
    )
    bt["y"] = 2e5
    bt["yhat"] = 2.1e5
    monthly = _aggregate_backtest_monthly(bt, "D")
    assert monthly["month"].dt.strftime("%Y-%m").tolist() == ["2026-03", "2026-04"]


def test_backtest_metrics_mape_is_percentage(daily, weekly):
    table, _ = rb.build_backtest_metrics(daily, weekly)
    for value in table["MAPE"]:
        assert value.endswith(" %")
        number = float(value.removesuffix(" %").replace(",", "."))
        assert 0 <= number < 100  # sensible percentage, not an exploded ratio


def test_backtest_info_lists_window_and_months(daily, weekly):
    info = rb.build_backtest_info(daily, weekly)
    values = " ".join(info["Valeur"].astype(str))
    assert "2026-02" in values  # backtest months present in the fixtures
    assert "→" in values  # the config window


def test_glossary_defines_every_metric():
    glossary = rb.backtest_metric_glossary()
    assert set(glossary["Métrique"]) >= {"MAE", "RMSE", "MAPE", "Biais (€)", "Biais (%)", "Tendance"}


def test_metrics_evolution_flags_aberrant_mape():
    records = [
        {
            "model": "Prophet Daily",
            "date": pd.Timestamp("2026-04-01"),
            "MAPE": 0.15,
            "MAE": 1e5,
            "RMSE": 2e5,
            "run_name": "ok",
        },
        {
            "model": "Prophet Daily",
            "date": pd.Timestamp("2026-10-01"),
            "MAPE": 2085.7,
            "MAE": 2e6,
            "RMSE": 2e6,
            "run_name": "bad",
        },
    ]
    table = rb.build_metrics_evolution(records)
    mape_by_run = dict(zip(table["Run"], table["MAPE"], strict=True))
    assert "⚠" in mape_by_run["bad"]  # degenerate value flagged
    assert "⚠" not in mape_by_run["ok"]


def test_plot_metrics_evolution_excludes_aberrant(tmp_path):
    # Only an aberrant point -> nothing plottable -> no chart file.
    only_bad = [
        {
            "model": "Prophet Daily",
            "date": pd.Timestamp("2026-10-01"),
            "MAPE": 2085.7,
            "MAE": 2e6,
            "RMSE": 2e6,
            "run_name": "bad",
        }
    ]
    assert rb.plot_metrics_evolution(only_bad, tmp_path) is None
    # A healthy point remains -> chart produced.
    mixed = [
        *only_bad,
        {
            "model": "Prophet Daily",
            "date": pd.Timestamp("2026-04-01"),
            "MAPE": 0.15,
            "MAE": 1e5,
            "RMSE": 2e5,
            "run_name": "ok",
        },
    ]
    path = rb.plot_metrics_evolution(mixed, tmp_path)
    assert path is not None
    assert path.exists()


def test_write_workbook_embeds_images(tmp_path, daily, weekly, real_df):
    import openpyxl

    records = [
        {
            "model": "Prophet Daily",
            "date": pd.Timestamp("2026-04-01"),
            "MAPE": 0.05,
            "MAE": 1e5,
            "RMSE": 2e5,
            "run_name": "r",
        }
    ]
    past = pd.DataFrame(
        {
            "forecast_date": pd.date_range("2026-06-01", periods=3, freq="MS").tolist() * 2,
            "prediction": [6e6] * 6,
            "run_name": ["a"] * 3 + ["b"] * 3,
        }
    )
    evolution_png = rb.plot_metrics_evolution(records, tmp_path)
    comparison_png = rb.plot_runs_comparison(past, tmp_path)
    sheets = {
        "Backtest métriques": {
            "blocks": [
                ("Métriques", rb.build_backtest_metrics(daily, weekly)[0]),
                ("Définitions", rb.backtest_metric_glossary()),
            ]
        },
        "Évolution métriques": {
            "blocks": [("Évolution", rb.build_metrics_evolution(records))],
            "images": [(evolution_png, None)],
        },
        "Comparaison runs": {
            "blocks": [("Comparaison", rb.build_runs_comparison(past))],
            "images": [(comparison_png, None)],
        },
    }
    path = tmp_path / "workbook.xlsx"
    rb.write_workbook(path, sheets)
    book = openpyxl.load_workbook(path)
    assert set(book.sheetnames) == {"Backtest métriques", "Évolution métriques", "Comparaison runs"}
    assert len(book["Évolution métriques"]._images) == 1
    assert len(book["Comparaison runs"]._images) == 1
