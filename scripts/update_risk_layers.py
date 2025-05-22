#!/usr/bin/env python3
"""Refresh risk scores for road segments."""

from pathfinder import update_risk_table, get_engine


def main():
    engine = get_engine()
    update_risk_table(engine)
    print("✅ road_risk_scores table updated")


if __name__ == "__main__":
    main()
