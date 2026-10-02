"""Step 6: charts.

    ./venv/bin/python charts.py   -> charts/01_fee_check.png, 02_trips.png, 03_speed_by_hour.png, 04_monthly.png, 05_engines.png
"""
import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

HERE = Path(__file__).parent
R = HERE / "results"
INK, DIM, GRID, BG, BLUE, ORANGE, GRAY = "#f2f2f0", "#8a8a87", "#1d1d1d", "#0b0b0b", "#3987e5", "#d95926", "#9a9a96"
plt.rcParams.update({
    "figure.facecolor": BG, "axes.facecolor": BG, "savefig.facecolor": BG, "text.color": INK,
    "axes.edgecolor": "#3a3a3a", "axes.labelcolor": DIM, "xtick.color": DIM, "ytick.color": DIM,
    "axes.grid": True, "axes.axisbelow": True, "grid.color": GRID, "grid.linewidth": 1,
    "axes.spines.top": False, "axes.spines.right": False,
    "font.family": ["Helvetica Neue", "Arial", "DejaVu Sans"], "font.size": 11, "axes.titlesize": 13,
    "axes.titlelocation": "left", "axes.titlepad": 12,
})
SERVICE = {"fhvhv": "Uber/Lyft", "yellow": "Yellow taxi"}
FLOWS = ["within", "into", "out of", "outside"]


def save(fig, name):
    fig.tight_layout()
    fig.savefig(HERE / "charts" / name, dpi=150)
    plt.close(fig)


def main():
    (HERE / "charts").mkdir(exist_ok=True)

    f = pd.read_csv(R / "fee_check.csv")
    fig, ax = plt.subplots(figsize=(10, 3.4))
    x = np.arange(4)
    for k, (svc, col) in enumerate([("fhvhv", BLUE), ("yellow", ORANGE)]):
        v = f[f["service"] == svc].set_index("flow").loc[FLOWS, "pct_paid_fee"]
        ax.bar(x + (k - 0.5) * 0.38, v, width=0.38, color=col, label=SERVICE[svc])
        for xi, val in zip(x + (k - 0.5) * 0.38, v):
            ax.text(xi, val + 1.5, f"{val:.1f}%", ha="center", color=INK, fontsize=9)
    ax.set_xticks(x, [f"{fl} the zone" if fl != "outside" else "outside (incl. passing through)" for fl in FLOWS])
    ax.set_ylim(0, 115); ax.set_ylabel("2025 trips that paid the CBD fee (%)")
    ax.legend(frameon=False, labelcolor=INK, fontsize=9, loc="upper right"); ax.grid(axis="x", visible=False)
    ax.set_title("The zone found in the data matches the fee: 37 taxi zones, Manhattan below 60th Street")
    save(fig, "01_fee_check.png")

    d = pd.read_csv(R / "before_after.csv")
    fig, axes = plt.subplots(1, 2, figsize=(11, 3.8), sharey=True)
    for ax, svc in zip(axes, ["fhvhv", "yellow"]):
        v = d[d["service"] == svc].set_index("flow").loc[FLOWS]
        cols = [GRAY if fl == "outside" else (ORANGE if c < 0 else BLUE) for fl, c in zip(FLOWS, v["trips_change_pct"])]
        ax.bar(FLOWS, v["trips_change_pct"], color=cols, width=0.55)
        for i, c in enumerate(v["trips_change_pct"]):
            ax.text(i, c + (0.6 if c >= 0 else -1.6), f"{c:+.1f}%", ha="center", color=INK, fontsize=10)
        ax.axhline(0, color=DIM, linewidth=1); ax.grid(axis="x", visible=False)
        ax.set_title(SERVICE[svc], fontsize=12)
    axes[0].set_ylabel("trips, Jan-Jun 2025 vs 2024")
    axes[0].set_ylim(-11, 22)
    fig.suptitle("Trips touching the zone fell against trips elsewhere (gray: outside the zone)", x=0.01, ha="left", fontsize=13, color=INK)
    save(fig, "02_trips.png")

    h = pd.read_csv(R / "speed_by_hour.csv")
    h = h[h["weekday"]].pivot_table(index="hour", columns=["flow", "year"], values="mph")
    fig, ax = plt.subplots(figsize=(10, 3.8))
    ax.axvspan(4.5, 20.5, color="#16202c", zorder=0, label="weekday peak toll hours (5 am - 9 pm)")
    for flow, col in [("within", BLUE), ("outside", GRAY)]:
        chg = 100 * (h[(flow, 2025)] / h[(flow, 2024)] - 1)
        ax.plot(chg.index, chg, color=col, marker="o", markersize=3, linewidth=2,
                label="trips within the zone" if flow == "within" else "trips outside the zone")
    ax.axhline(0, color=DIM, linewidth=1)
    ax.set_xticks(range(0, 24, 3)); ax.set_xlabel("hour of pickup, weekdays"); ax.set_ylabel("average speed, 2025 vs 2024 (%)")
    ax.legend(frameon=False, labelcolor=INK, fontsize=9, loc="lower left", ncol=3); ax.set_ylim(-4, 4.5)
    ax.set_title("Weekday trip speeds rose a little inside the zone, mostly while the toll applies")
    save(fig, "03_speed_by_hour.png")

    m = pd.read_csv(R / "flows_monthly.csv")
    m = m[m["flow"] != "outside"].groupby(["service", "year", "month"])["trips"].sum().reset_index()
    fig, axes = plt.subplots(1, 2, figsize=(11, 3.6))
    for ax, svc in zip(axes, ["fhvhv", "yellow"]):
        for year, col in [(2024, GRAY), (2025, BLUE)]:
            v = m[(m["service"] == svc) & (m["year"] == year)]
            ax.plot(v["month"], v["trips"] / 1e6, color=col, marker="o", linewidth=2, label=str(year))
        ax.set_xticks(range(1, 7), ["Jan", "Feb", "Mar", "Apr", "May", "Jun"]); ax.set_title(SERVICE[svc], fontsize=12)
        ax.legend(frameon=False, labelcolor=INK, fontsize=9)
    axes[0].set_ylabel("trips touching the zone (millions)")
    fig.suptitle("Trips within, into or out of the zone, month by month", x=0.01, ha="left", fontsize=13, color=INK)
    save(fig, "04_monthly.png")

    c = json.loads((R / "timing_curate.json").read_text())["spark_seconds_raw_to_curated"]
    mt = json.loads((R / "timing_metrics.json").read_text())["spark_seconds_curated_to_metrics"]
    dk = json.loads((R / "duckdb_check.json").read_text())["duckdb_seconds_raw_to_flows"]
    fig, ax = plt.subplots(figsize=(10, 2.8))
    ax.barh(["Spark (local, 12 cores)", "DuckDB (same laptop)"], [c, dk], color=[BLUE, GRAY], height=0.5)
    ax.barh(["Spark (local, 12 cores)"], [mt], left=[c], color=ORANGE, height=0.5)
    ax.text(c + mt + 4, 0, f"{c:.0f} s clean + write Parquet, then {mt:.0f} s for every metric", va="center", color=INK, fontsize=9)
    ax.text(dk + 4, 1, f"{dk:.0f} s raw files to monthly flows, in one SQL pass", va="center", color=INK, fontsize=9)
    ax.set_xlim(0, (c + mt) * 1.9); ax.set_xlabel("seconds for 286 million trips (6.6 GB of Parquet)"); ax.grid(axis="y", visible=False)
    ax.set_title("On one laptop a single-node engine is faster; Spark's case is that the same code scales out")
    save(fig, "05_engines.png")
    print("charts written")


if __name__ == "__main__":
    main()
