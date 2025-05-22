#!/usr/bin/env python3
"""Command line tool to compute a risk-aware route."""

import argparse
from pathfinder.risk_tsp import plan_route, get_engine


def main():
    ap = argparse.ArgumentParser(description="Risk-aware route optimisation")
    ap.add_argument("--limit", type=int, default=20,
                    help="Number of primary roads to include")
    ap.add_argument("--alpha", type=float, default=1.0,
                    help="Risk weight multiplier")
    args = ap.parse_args()

    df = plan_route(limit=args.limit, alpha=args.alpha, engine=get_engine())
    print(df[["order", "road_id", "events", "risk"]])


if __name__ == "__main__":
    main()
