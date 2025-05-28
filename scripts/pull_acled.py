#!/usr/bin/env python3
"""Pathfinder – universal ACLED puller."""

from pathfinder.etl.pull_acled import main, list_countries
import sys

if __name__ == "__main__":
    args = sys.argv[1:]
    if "--list" in args:
        from os import getenv
        token = getenv("ACLED_TOKEN")
        email = getenv("ACLED_EMAIL")
        if token and email:
            for name in list_countries(token, email):
                print(name)
        else:
            print("ACLED_TOKEN and ACLED_EMAIL must be set")
        sys.exit(0)
    main(args)
