#!/usr/bin/env bash
set -euo pipefail
mkdir -p scratch data/raw

API="https://data.humdata.org/api/3/action/package_show?id=sudan-acled-conflict-data"
META=scratch/hdx_sudan_acled_meta.json

curl -s "$API" -o "$META"

url=$(jq -r '.result.resources[]
             | select(.name|test("political_violence";"i"))
             | .url' "$META")

fname=data/raw/$(basename "$url")
curl -L -o "$fname" "$url"

echo "✅  Saved $fname"
