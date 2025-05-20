#!/usr/bin/env python3
"""
Generate a simple PNG chart of monthly totals using data in PostGIS.
Outputs to output.png in the repo root.
"""
import matplotlib.pyplot as plt
from pathfinder.queries import monthly_totals


def main():
    df = monthly_totals()
    if df.empty:
        raise SystemExit("No data returned from monthly_totals()")

    fig, ax1 = plt.subplots(figsize=(8, 4))
    ax2 = ax1.twinx()
    ax1.plot(df["month_start"], df["events"], "-o", color="tab:blue", label="Events")
    ax2.plot(df["month_start"], df["fatalities"], "-o", color="tab:red", label="Fatalities")

    ax1.set_xlabel("Month")
    ax1.set_ylabel("Events", color="tab:blue")
    ax2.set_ylabel("Fatalities", color="tab:red")
    fig.tight_layout()
    fig.savefig("output.png")
    print("✅  Saved output.png")


if __name__ == "__main__":
    main()
