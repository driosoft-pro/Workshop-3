#!/usr/bin/env python
"""Render the four required KPIs (+ pipeline health) as PNG screenshots.

Every chart queries the PostgreSQL **views** defined in ``sql/kpis.sql`` —
never CSV files — which is exactly what the Superset dashboard does. Use it
to regenerate ``dashboards/screenshots/*.png`` after each streaming run::

    python dashboards/kpi_charts.py
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
from sqlalchemy import create_engine

from src import config

OUT_DIR = Path(__file__).resolve().parent / "screenshots"


def query(engine, view: str) -> pd.DataFrame:
    return pd.read_sql(f"SELECT * FROM {view}", engine)


def chart_kpi1(engine, out: Path) -> None:
    """KPI 1 — average prediction error (MAE), RMSE, bias."""
    kpi = query(engine, "vw_kpi_avg_error").iloc[0]
    fig, axes = plt.subplots(1, 4, figsize=(14, 3.2))
    values = [kpi["mae"], kpi["rmse"], kpi["bias"], kpi["n_predictions"]]
    labels = ["MAE", "RMSE", "Bias\n(pred − actual)", "Predictions"]
    colors = ["steelblue", "seagreen", "indianred", "gray"]
    for ax, value, label, color in zip(axes, values, labels, colors):
        ax.barh([0], [float(value)], color=color)
        ax.set_yticks([])
        ax.set_title(f"{float(value):,.3f}", fontsize=16)
        ax.set_xlabel(label)
        ax.spines[["top", "right", "left"]].set_visible(False)
    fig.suptitle("KPI 1 — Average prediction error (vw_kpi_avg_error)", y=1.05)
    fig.tight_layout()
    fig.savefig(out, dpi=130, bbox_inches="tight")
    plt.close(fig)


def chart_kpi2(engine, out: Path) -> None:
    """KPI 2 — predictions by country."""
    df = query(engine, "vw_kpi_predictions_by_country")
    top = df.sort_values("n_predictions", ascending=False).head(15)
    fig, ax = plt.subplots(figsize=(9, 6))
    ax.barh(top["country_name"][::-1], top["n_predictions"][::-1], color="steelblue")
    for _, row in top.iterrows():
        ax.text(row["n_predictions"] + 0.2, row["country_name"],
                f"{int(row['n_predictions'])} (MAE {row['mae']:.2f})",
                va="center", fontsize=8)
    ax.set_xlabel("number of predictions")
    ax.set_title(f"KPI 2 — Predictions by country (top 15 of {len(df)}) — vw_kpi_predictions_by_country")
    ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    fig.savefig(out, dpi=130, bbox_inches="tight")
    plt.close(fig)


def chart_kpi3(engine, out: Path) -> None:
    """KPI 3 — predicted vs actual score."""
    df = query(engine, "vw_kpi_pred_vs_actual")
    fig, ax = plt.subplots(figsize=(7, 6.5))
    ax.scatter(df["actual_score"], df["predicted_score"], alpha=0.55,
               color="steelblue", s=28)
    lo = float(df[["actual_score", "predicted_score"]].min().min()) - 0.2
    hi = float(df[["actual_score", "predicted_score"]].max().max()) + 0.2
    ax.plot([lo, hi], [lo, hi], "r--", lw=1.2, label="y = x (perfect)")
    ax.set_xlabel("actual happiness score")
    ax.set_ylabel("predicted happiness score")
    ax.set_title(f"KPI 3 — Predicted vs actual ({len(df)} predictions) — vw_kpi_pred_vs_actual")
    ax.legend()
    ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    fig.savefig(out, dpi=130, bbox_inches="tight")
    plt.close(fig)


def chart_kpi4(engine, out: Path) -> None:
    """KPI 4 — prediction trends over time (observation year)."""
    df = query(engine, "vw_kpi_trend_by_year")
    fig, ax1 = plt.subplots(figsize=(9, 4.5))
    ax1.plot(df["observation_year"], df["avg_actual"], marker="o",
             label="actual (avg)", color="steelblue")
    ax1.plot(df["observation_year"], df["avg_predicted"], marker="s",
             label="predicted (avg)", color="seagreen")
    ax1.set_xlabel("observation year")
    ax1.set_ylabel("average happiness score")
    ax2 = ax1.twinx()
    ax2.bar(df["observation_year"], df["mae"], alpha=0.25, color="indianred",
            width=0.15, label="MAE")
    ax2.set_ylabel("MAE")
    ax1.set_title("KPI 4 — Trends over time — vw_kpi_trend_by_year")
    lines = ax1.get_lines() + ax2.containers
    ax1.legend(lines, [l.get_label() for l in lines], loc="lower left")
    fig.tight_layout()
    fig.savefig(out, dpi=130, bbox_inches="tight")
    plt.close(fig)


def chart_status(engine, out: Path) -> None:
    """Pipeline health — the contract validation in action."""
    df = query(engine, "vw_pipeline_status")
    fig, ax = plt.subplots(figsize=(8, 4))
    bars = ax.bar(df["processing_status"], df["n_events"],
                  color=["indianred", "indianred", "orange", "gray", "seagreen"][: len(df)])
    ax.bar_label(bars, padding=2)
    ax.set_ylabel("events")
    ax.set_title("Pipeline status — every raw event is stored (vw_pipeline_status)")
    ax.spines[["top", "right"]].set_visible(False)
    plt.xticks(rotation=15)
    fig.tight_layout()
    fig.savefig(out, dpi=130, bbox_inches="tight")
    plt.close(fig)


def main() -> int:
    engine = create_engine(config.DATABASE_URL)
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    charts = {
        "kpi1_avg_error.png": chart_kpi1,
        "kpi2_predictions_by_country.png": chart_kpi2,
        "kpi3_pred_vs_actual.png": chart_kpi3,
        "kpi4_trend_by_year.png": chart_kpi4,
        "kpi5_pipeline_status.png": chart_status,
    }
    for name, fn in charts.items():
        fn(engine, OUT_DIR / name)
        print(f"wrote {OUT_DIR / name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
