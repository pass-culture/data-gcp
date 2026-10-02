"""Build the French finance "compte rendu" from two Prophet runs (daily + weekly).

This module contains pure, side-effect-light helpers that turn the raw data
loaded from MLflow and BigQuery into:
- a multi-sheet Excel workbook,
- PNG charts,
- a French markdown summary (plus a one-line Slack summary).

The orchestration (MLflow / BigQuery / GCS IO) lives in ``generate_report.py``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.ticker import FuncFormatter
from sklearn.metrics import (
    mean_absolute_error,
    mean_absolute_percentage_error,
    root_mean_squared_error,
)

from forecast.utils.constants import PRICING_LOWER_BOUND, PRICING_UPPER_BOUND

if TYPE_CHECKING:
    from pathlib import Path

REAL_COL = "Pricing réel (€)"
DAILY_COL = "Prophet Daily (€)"
WEEKLY_COL = "Prophet Weekly (€)"


@dataclass
class ModelReportData:
    """Container for everything the report needs about a single model run."""

    label: str  # Human label, e.g. "Prophet Daily"
    model_name: str  # e.g. "daily_pricing"
    model_type: str  # e.g. "prophet"
    run_id: str
    run_name: str
    params: dict = field(default_factory=dict)
    config: dict = field(default_factory=dict)
    # Monthly series over [year start, horizon]: columns month (Timestamp), total_pricing
    full_monthly: pd.DataFrame = field(default_factory=pd.DataFrame)
    # Monthly backtest aggregation: columns month (Timestamp), y, yhat
    backtest_monthly: pd.DataFrame = field(default_factory=pd.DataFrame)


def format_eur(value: float | None) -> str:
    """Format a number as euros with space thousands separators (French style)."""
    if value is None or pd.isna(value):
        return "n/a"
    return f"{value:,.0f}".replace(",", " ") + " €"


def _format_pct(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "n/a"
    return f"{value * 100:.1f} %".replace(".", ",")


def _eur_axis_formatter() -> FuncFormatter:
    return FuncFormatter(lambda x, _pos: f"{x:,.0f}".replace(",", " "))


# --------------------------------------------------------------------------- #
# Tidy monthly table (internal, timestamp-indexed) + display version
# --------------------------------------------------------------------------- #
def build_tidy_monthly(
    daily: ModelReportData,
    weekly: ModelReportData,
    real_df: pd.DataFrame,
) -> pd.DataFrame:
    """Merge both models' monthly forecasts with observed pricing.

    Args:
        daily: Daily model report data (``full_monthly`` with month, total_pricing).
        weekly: Weekly model report data.
        real_df: Observed monthly pricing, columns ``month`` (Timestamp), ``real_total``.

    Returns:
        DataFrame with columns: month (Timestamp), real, daily, weekly.
    """
    d = daily.full_monthly.rename(columns={"total_pricing": "daily"})[["month", "daily"]]
    w = weekly.full_monthly.rename(columns={"total_pricing": "weekly"})[["month", "weekly"]]
    r = real_df.rename(columns={"real_total": "real"})[["month", "real"]]

    tidy = pd.merge(d, w, on="month", how="outer")
    tidy = pd.merge(tidy, r, on="month", how="left")
    tidy = tidy.sort_values("month").reset_index(drop=True)
    return tidy


def build_monthly_sheet(tidy: pd.DataFrame) -> pd.DataFrame:
    """Build the display monthly sheet (French columns, YYYY-MM month labels)."""
    out = tidy.copy()
    out["Mois"] = out["month"].dt.strftime("%Y-%m")
    out = out.rename(columns={"real": REAL_COL, "daily": DAILY_COL, "weekly": WEEKLY_COL})
    cols = ["Mois", REAL_COL, DAILY_COL, WEEKLY_COL]
    out = out[cols].copy()
    for col in (REAL_COL, DAILY_COL, WEEKLY_COL):
        out[col] = out[col].round(0)
    return out


# --------------------------------------------------------------------------- #
# Quarterly and annual totals
# --------------------------------------------------------------------------- #
def build_quarterly_annual(tidy: pd.DataFrame, years: list[int]) -> pd.DataFrame:
    """Aggregate monthly values into quarterly and annual totals for given years.

    Observed pricing is summed only over months where it is available; model
    columns are always summed over the full period.

    Args:
        tidy: Output of :func:`build_tidy_monthly`.
        years: Years to include (e.g. ``[2026, 2027]``).

    Returns:
        DataFrame with columns: Période, Pricing réel (€), Prophet Daily (€),
        Prophet Weekly (€).
    """
    df = tidy.copy()
    df["year"] = df["month"].dt.year
    df["quarter"] = df["month"].dt.quarter
    df = df[df["year"].isin(years)]

    rows: list[dict] = []
    for year in years:
        year_df = df[df["year"] == year]
        if year_df.empty:
            continue
        for quarter in sorted(year_df["quarter"].unique()):
            q_df = year_df[year_df["quarter"] == quarter]
            rows.append(
                {
                    "Période": f"T{quarter} {year}",
                    REAL_COL: q_df["real"].sum(min_count=1),
                    DAILY_COL: q_df["daily"].sum(min_count=1),
                    WEEKLY_COL: q_df["weekly"].sum(min_count=1),
                }
            )
        rows.append(
            {
                "Période": f"Total {year}",
                REAL_COL: year_df["real"].sum(min_count=1),
                DAILY_COL: year_df["daily"].sum(min_count=1),
                WEEKLY_COL: year_df["weekly"].sum(min_count=1),
            }
        )

    out = pd.DataFrame(rows)
    for col in (REAL_COL, DAILY_COL, WEEKLY_COL):
        out[col] = out[col].round(0)
    return out


# --------------------------------------------------------------------------- #
# Backtest metrics + over/under prediction
# --------------------------------------------------------------------------- #
def _metrics_from_monthly(backtest_monthly: pd.DataFrame) -> dict:
    """Compute monthly-level MAE/RMSE/MAPE and bias on a backtest aggregation."""
    valid = backtest_monthly.dropna(subset=["y", "yhat"])
    if valid.empty:
        return {"MAE": None, "RMSE": None, "MAPE": None, "bias_eur": None, "bias_pct": None}
    y = valid["y"]
    yhat = valid["yhat"]
    total_real = y.sum()
    total_pred = yhat.sum()
    bias_eur = total_pred - total_real
    bias_pct = bias_eur / total_real if total_real else None
    return {
        "MAE": mean_absolute_error(y, yhat),
        "RMSE": root_mean_squared_error(y, yhat),
        "MAPE": mean_absolute_percentage_error(y, yhat),
        "bias_eur": bias_eur,
        "bias_pct": bias_pct,
    }


def build_backtest_detail(daily: ModelReportData, weekly: ModelReportData) -> pd.DataFrame:
    """Build the monthly backtest detail sheet (observed vs predicted per model)."""
    d = daily.backtest_monthly.rename(columns={"y": "Réel (€)", "yhat": DAILY_COL})
    w = weekly.backtest_monthly.rename(columns={"y": "Réel weekly (€)", "yhat": WEEKLY_COL})
    merged = pd.merge(
        d[["month", "Réel (€)", DAILY_COL]],
        w[["month", WEEKLY_COL]],
        on="month",
        how="outer",
    ).sort_values("month")
    merged["Mois"] = merged["month"].dt.strftime("%Y-%m")
    out = merged[["Mois", "Réel (€)", DAILY_COL, WEEKLY_COL]].copy()
    for col in ("Réel (€)", DAILY_COL, WEEKLY_COL):
        out[col] = out[col].round(0)
    return out


def build_backtest_metrics(daily: ModelReportData, weekly: ModelReportData) -> tuple[pd.DataFrame, dict]:
    """Build the backtest metrics summary table and a dict of raw metrics.

    Returns:
        Tuple of (display DataFrame, {"Prophet Daily": metrics, "Prophet Weekly": metrics}).
    """
    metrics = {
        daily.label: _metrics_from_monthly(daily.backtest_monthly),
        weekly.label: _metrics_from_monthly(weekly.backtest_monthly),
    }
    rows = []
    for label, m in metrics.items():
        tendance = "—"
        if m["bias_pct"] is not None:
            tendance = "Surestimation" if m["bias_pct"] > 0 else "Sous-estimation"
        rows.append(
            {
                "Modèle": label,
                "MAE": None if m["MAE"] is None else round(m["MAE"], 0),
                "RMSE": None if m["RMSE"] is None else round(m["RMSE"], 0),
                "MAPE": _format_pct(m["MAPE"]),
                "Biais (€)": None if m["bias_eur"] is None else round(m["bias_eur"], 0),
                "Biais (%)": _format_pct(m["bias_pct"]),
                "Tendance": tendance,
            }
        )
    return pd.DataFrame(rows), metrics


def _backtest_months(data: ModelReportData) -> list[str]:
    """Return the sorted list of complete backtest months (YYYY-MM) for a model."""
    if data.backtest_monthly.empty:
        return []
    return sorted(data.backtest_monthly["month"].dt.strftime("%Y-%m").unique())


def build_backtest_info(daily: ModelReportData, weekly: ModelReportData) -> pd.DataFrame:
    """Build a small sheet block describing the backtest window and months used."""
    months = sorted(set(_backtest_months(daily)) | set(_backtest_months(weekly)))
    rows = [
        {
            "Information": "Fenêtre de backtest (config)",
            "Valeur": f"{daily.params.get('backtest_start_date', 'n/a')} → "
            f"{daily.params.get('backtest_end_date', 'n/a')}",
        },
        {
            "Information": "Mois complets évalués",
            "Valeur": ", ".join(months) if months else "aucun",
        },
        {
            "Information": "Granularité des métriques",
            "Valeur": "Agrégation mensuelle (sommes du réel et des prévisions par mois)",
        },
    ]
    return pd.DataFrame(rows)


def backtest_metric_glossary() -> pd.DataFrame:
    """Return the definition of each backtest metric (French)."""
    rows = [
        ("MAE", "Erreur absolue moyenne : moyenne des écarts absolus mensuels entre réel et prévision (en €)."),
        (
            "RMSE",
            "Racine de l'erreur quadratique moyenne : pénalise davantage les gros écarts mensuels (en €).",
        ),
        (
            "MAPE",
            "Erreur absolue moyenne en pourcentage : moyenne des écarts absolus rapportés au réel mensuel (en %).",
        ),
        ("Biais (€)", "Somme des prévisions moins la somme du réel sur les mois évalués ; positif = surestimation."),
        ("Biais (%)", "Biais rapporté au total réel observé sur les mois évalués."),
        ("Tendance", "Sens du biais : « Surestimation » si positif, « Sous-estimation » si négatif."),
    ]
    return pd.DataFrame(rows, columns=["Métrique", "Définition"])


def build_metrics_evolution(evolution_records: list[dict]) -> pd.DataFrame:
    """Build the metrics-evolution sheet from past runs.

    Args:
        evolution_records: list of dicts with keys ``model``, ``date``, ``run_name``,
            ``MAE``, ``RMSE``, ``MAPE``.

    Returns:
        Display DataFrame sorted by model then date.
    """
    if not evolution_records:
        return pd.DataFrame(columns=["Modèle", "Date du run", "Run", "MAE", "RMSE", "MAPE"])
    df = pd.DataFrame(evolution_records).sort_values(["model", "date"]).reset_index(drop=True)
    df["MAPE"] = df["MAPE"].apply(_format_pct)
    df["MAE"] = df["MAE"].round(0)
    df["RMSE"] = df["RMSE"].round(0)
    df = df.rename(columns={"model": "Modèle", "date": "Date du run", "run_name": "Run"})
    return df[["Modèle", "Date du run", "Run", "MAE", "RMSE", "MAPE"]]


# --------------------------------------------------------------------------- #
# Forecast comparison across runs
# --------------------------------------------------------------------------- #
def build_runs_comparison(past_monthly_forecasts: pd.DataFrame) -> pd.DataFrame:
    """Pivot past monthly forecasts into one column per run for side-by-side review."""
    if past_monthly_forecasts.empty:
        return pd.DataFrame()
    df = past_monthly_forecasts.copy()
    df["forecast_date"] = pd.to_datetime(df["forecast_date"])
    pivot = df.pivot_table(
        index="forecast_date",
        columns="run_name",
        values="prediction",
        aggfunc="mean",
    ).sort_index()
    pivot = pivot.round(0).reset_index()
    pivot["forecast_date"] = pivot["forecast_date"].dt.strftime("%Y-%m")
    pivot = pivot.rename(columns={"forecast_date": "Mois"})
    return pivot


# --------------------------------------------------------------------------- #
# Model configuration summary
# --------------------------------------------------------------------------- #
def _config_rows(data: ModelReportData) -> dict:
    params = data.params
    prophet_cfg = (data.config or {}).get("prophet", {})
    eval_cfg = (data.config or {}).get("evaluation", {})
    changepoints = prophet_cfg.get("changepoints") or []
    return {
        "Modèle": data.label,
        "Nom config": data.model_name,
        "Début entraînement": params.get("train_start_date", "n/a"),
        "Fin entraînement": params.get("backtest_start_date", "n/a"),
        "Début backtest": params.get("backtest_start_date", "n/a"),
        "Fin backtest": params.get("backtest_end_date", "n/a"),
        "Horizon de prévision": params.get("forecast_horizon_date", "n/a"),
        "Fréquence": eval_cfg.get("freq", "n/a"),
        "Croissance (growth)": prophet_cfg.get("growth", "n/a"),
        "Nb changepoints (config)": len(changepoints),
        "Saisonnalité annuelle": prophet_cfg.get("yearly_seasonality", "n/a"),
        "Saisonnalité hebdo": prophet_cfg.get("weekly_seasonality", "n/a"),
        "Intervalle d'incertitude": prophet_cfg.get("interval_width", "n/a"),
    }


def build_config_summary(daily: ModelReportData, weekly: ModelReportData) -> pd.DataFrame:
    """Build the configuration summary sheet (one column per model)."""
    rows_daily = _config_rows(daily)
    rows_weekly = _config_rows(weekly)
    keys = list(rows_daily.keys())[1:]  # drop "Modèle" from index rows
    out = pd.DataFrame(
        {
            "Paramètre": keys,
            daily.label: [rows_daily[k] for k in keys],
            weekly.label: [rows_weekly[k] for k in keys],
        }
    )
    return out


# --------------------------------------------------------------------------- #
# Charts
# --------------------------------------------------------------------------- #
def _add_threshold_lines(ax, xmin, xmax) -> None:
    ax.hlines(
        y=PRICING_LOWER_BOUND,
        xmin=xmin,
        xmax=xmax,
        colors="orange",
        linestyles="--",
        alpha=0.7,
        label=f"Seuil {PRICING_LOWER_BOUND / 1e6:.0f} M€",
    )
    ax.hlines(
        y=PRICING_UPPER_BOUND,
        xmin=xmin,
        xmax=xmax,
        colors="red",
        linestyles="--",
        alpha=0.7,
        label=f"Seuil {PRICING_UPPER_BOUND / 1e6:.0f} M€",
    )


def plot_monthly(tidy: pd.DataFrame, output_dir: Path) -> Path:
    """Plot observed pricing vs both monthly forecasts."""
    fig, ax = plt.subplots(figsize=(13, 6))
    x = tidy["month"]
    ax.plot(x, tidy["real"], marker="o", label="Pricing réel")
    ax.plot(x, tidy["daily"], marker="o", label="Prophet Daily")
    ax.plot(x, tidy["weekly"], marker="o", label="Prophet Weekly")
    _add_threshold_lines(ax, x.min(), x.max())
    ax.set_title("Prévisions mensuelles de pricing vs réel")
    ax.set_xlabel("Mois")
    ax.set_ylabel("Pricing (€)")
    ax.yaxis.set_major_formatter(_eur_axis_formatter())
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.autofmt_xdate()
    fig.tight_layout()
    path = output_dir / "previsions_mensuelles.png"
    fig.savefig(path, dpi=120)
    plt.close(fig)
    return path


def plot_quarterly(quarterly: pd.DataFrame, output_dir: Path) -> Path:
    """Bar chart of quarterly totals per model (excluding annual total rows)."""
    q_only = quarterly[quarterly["Période"].str.startswith("T")].copy()
    fig, ax = plt.subplots(figsize=(13, 6))
    x = range(len(q_only))
    width = 0.35
    bars_daily = ax.bar([i - width / 2 for i in x], q_only[DAILY_COL], width=width, label="Prophet Daily")
    bars_weekly = ax.bar([i + width / 2 for i in x], q_only[WEEKLY_COL], width=width, label="Prophet Weekly")
    for bars in (bars_daily, bars_weekly):
        ax.bar_label(
            bars,
            labels=[format_eur(v) for v in bars.datavalues],
            padding=3,
            rotation=90,
            fontsize=8,
        )
    ax.set_xticks(list(x))
    ax.set_xticklabels(q_only["Période"], rotation=45, ha="right")
    ax.set_title("Totaux trimestriels prévus")
    ax.set_ylabel("Pricing (€)")
    ax.yaxis.set_major_formatter(_eur_axis_formatter())
    ax.margins(y=0.15)
    ax.legend()
    ax.grid(True, alpha=0.3, axis="y")
    fig.tight_layout()
    path = output_dir / "totaux_trimestriels.png"
    fig.savefig(path, dpi=120)
    plt.close(fig)
    return path


def plot_metrics_evolution(evolution_records: list[dict], output_dir: Path) -> Path | None:
    """Line chart of MAPE across past runs, per model."""
    if not evolution_records:
        return None
    df = pd.DataFrame(evolution_records)
    fig, ax = plt.subplots(figsize=(12, 6))
    for model, g in df.sort_values("date").groupby("model"):
        ax.plot(pd.to_datetime(g["date"]), g["MAPE"] * 100, marker="o", label=model)
    ax.set_title("Évolution du MAPE backtest par run")
    ax.set_xlabel("Date du run")
    ax.set_ylabel("MAPE (%)")
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.autofmt_xdate()
    fig.tight_layout()
    path = output_dir / "evolution_metriques.png"
    fig.savefig(path, dpi=120)
    plt.close(fig)
    return path


def plot_runs_comparison(past_monthly_forecasts: pd.DataFrame, output_dir: Path) -> Path | None:
    """Overlay the monthly forecast of each past run to show how it evolves."""
    if past_monthly_forecasts.empty:
        return None
    df = past_monthly_forecasts.copy()
    df["forecast_date"] = pd.to_datetime(df["forecast_date"])
    fig, ax = plt.subplots(figsize=(13, 6))
    for run_name, g in df.sort_values("forecast_date").groupby("run_name"):
        ax.plot(g["forecast_date"], g["prediction"], marker="o", linewidth=1.5, label=run_name)
    _add_threshold_lines(ax, df["forecast_date"].min(), df["forecast_date"].max())
    ax.set_title("Comparaison des prévisions entre runs")
    ax.set_xlabel("Mois")
    ax.set_ylabel("Pricing (€)")
    ax.yaxis.set_major_formatter(_eur_axis_formatter())
    ax.legend(bbox_to_anchor=(1.02, 1), loc="upper left", fontsize=8)
    ax.grid(True, alpha=0.3)
    fig.autofmt_xdate()
    fig.tight_layout()
    path = output_dir / "comparaison_runs.png"
    fig.savefig(path, dpi=120)
    plt.close(fig)
    return path


# --------------------------------------------------------------------------- #
# Excel + markdown
# --------------------------------------------------------------------------- #
def write_excel(path: Path, sheets: dict[str, pd.DataFrame]) -> None:
    """Write all sheets to a single multi-sheet Excel workbook (one table per sheet)."""
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for sheet_name, df in sheets.items():
            # Excel sheet names are capped at 31 characters.
            df.to_excel(writer, sheet_name=sheet_name[:31], index=False)


def _normalize_spec(spec) -> tuple[list, list]:
    """Normalise a sheet spec into (blocks, images).

    A spec is either a DataFrame (single block, no image) or a dict with optional
    ``blocks`` (list of ``(title_or_None, DataFrame)``) and ``images`` (list of
    ``(path_or_None, anchor_or_None)``).
    """
    if isinstance(spec, pd.DataFrame):
        return [(None, spec)], []
    blocks = spec.get("blocks", [])
    images = spec.get("images", [])
    return blocks, images


def write_workbook(path: Path, sheets: dict) -> None:
    """Write a multi-sheet workbook supporting stacked blocks and embedded charts.

    Args:
        path: Destination ``.xlsx`` path.
        sheets: Ordered mapping ``sheet_name -> spec`` (see :func:`_normalize_spec`).
    """
    from openpyxl.drawing.image import Image as XLImage

    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for raw_name, spec in sheets.items():
            name = raw_name[:31]
            blocks, images = _normalize_spec(spec)
            cursor = 0  # 0-indexed row for the next piece of content
            title_cells: list[tuple[int, str]] = []
            for title, df in blocks:
                if title is not None:
                    title_cells.append((cursor, title))
                    cursor += 1
                df.to_excel(writer, sheet_name=name, index=False, startrow=cursor)
                cursor += len(df) + 1  # header row + data rows
                cursor += 2  # spacer between blocks
            if name not in writer.sheets:
                writer.book.create_sheet(name)
            ws = writer.sheets[name]
            for row_0indexed, title in title_cells:
                ws.cell(row=row_0indexed + 1, column=1, value=title)
            for img_path, anchor in images:
                if img_path is None:
                    continue
                image = XLImage(str(img_path))
                # Scale down so wide charts fit comfortably in the sheet.
                if image.width > 900:
                    scale = 900 / image.width
                    image.width = 900
                    image.height = int(image.height * scale)
                ws.add_image(image, anchor or f"A{cursor + 1}")


def build_summary(
    *,
    report_year: int,
    next_year: int,
    quarterly: pd.DataFrame,
    backtest_metrics: dict,
    daily: ModelReportData,
    weekly: ModelReportData,
    n_past_runs: int,
) -> tuple[str, str]:
    """Build the French markdown summary and a one-line Slack summary.

    Returns:
        Tuple ``(markdown_text, one_line_summary)``.
    """

    def _annual_total(label_col: str, year: int) -> float | None:
        row = quarterly[quarterly["Période"] == f"Total {year}"]
        if row.empty:
            return None
        return row.iloc[0][label_col]

    daily_total_cur = _annual_total(DAILY_COL, report_year)
    weekly_total_cur = _annual_total(WEEKLY_COL, report_year)
    daily_total_next = _annual_total(DAILY_COL, next_year)
    weekly_total_next = _annual_total(WEEKLY_COL, next_year)

    def _verdict(label: str) -> str:
        m = backtest_metrics.get(label, {})
        if m.get("bias_pct") is None:
            return f"- **{label}** : pas de données de backtest exploitables."
        sens = "surestime" if m["bias_pct"] > 0 else "sous-estime"
        return (
            f"- **{label}** : {sens} le pricing réel de "
            f"{_format_pct(abs(m['bias_pct']))} sur le backtest "
            f"(MAPE {_format_pct(m['MAPE'])}, biais {format_eur(m['bias_eur'])})."
        )

    md = f"""# Compte rendu des prévisions de pricing — {report_year}

Rapport généré automatiquement après l'entraînement des modèles Prophet
(daily & weekly) pour l'équipe Finance (DAF).

## 1. Totaux annuels prévus

| Modèle | Total {report_year} | Total {next_year} |
| --- | --- | --- |
| Prophet Daily | {format_eur(daily_total_cur)} | {format_eur(daily_total_next)} |
| Prophet Weekly | {format_eur(weekly_total_cur)} | {format_eur(weekly_total_next)} |

Ces totaux correspondent à la **somme des prévisions du modèle** sur l'ensemble
de l'année. Pour {report_year}, les mois déjà écoulés sont des prévisions
in-sample (période d'entraînement) et peuvent être comparés au réalisé observé
dans l'onglet `Prévisions mensuelles`. Le rapport couvre **{report_year} et
{next_year} en entier** (l'horizon opérationnel du modèle n'est pas utilisé ici).

## 2. Qualité des prévisions (backtest)

{_verdict(daily.label)}
{_verdict(weekly.label)}

Un biais positif indique une **surestimation**, un biais négatif une
**sous-estimation** du pricing réel. Les métriques sont calculées sur les mois
complets de la fenêtre de backtest (voir l'onglet `Backtest métriques`).

## 3. Configuration des modèles

- **Période d'entraînement** : {daily.params.get("train_start_date", "n/a")} →
  {daily.params.get("backtest_start_date", "n/a")}
- **Fenêtre de backtest** : {daily.params.get("backtest_start_date", "n/a")} →
  {daily.params.get("backtest_end_date", "n/a")}
- **Période couverte par le rapport** : {report_year}-01 → {next_year}-12

## 4. Contenu du classeur Excel

- `Prévisions mensuelles` : prévisions mensuelles des deux modèles et pricing
  réel sur {report_year} et {next_year}.
- `Totaux trim. & annuels` : totaux trimestriels et annuels {report_year}/{next_year}.
- `Backtest détail` / `Backtest métriques` : performance sur le backtest (mois
  considérés, définition des métriques et tendance sur/sous-estimation).
- `Évolution métriques` : évolution des métriques sur les {n_past_runs} derniers
  runs (graphique inclus).
- `Comparaison runs` : prévisions mensuelles des runs précédents (graphique inclus).
- `Configuration` : récapitulatif de la configuration des modèles.

_Note : les modèles sont entraînés sur les premiers mois de {report_year} ; les
prévisions sur cette période correspondent donc à un ajustement in-sample._
"""

    one_line = (
        f":moneybag: Compte rendu pricing {report_year} généré | "
        f"Total {report_year} — Daily: {format_eur(daily_total_cur)}, "
        f"Weekly: {format_eur(weekly_total_cur)} | "
        f"Total {next_year} — Daily: {format_eur(daily_total_next)}, "
        f"Weekly: {format_eur(weekly_total_next)}"
    )
    return md, one_line
