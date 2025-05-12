import requests, pandas as pd, sys, re, json
from pathlib import Path
from sqlalchemy import create_engine

DATASET = "fcbb990b-bed4-4421-8240-472429b2d3dc"  # SA political violence dataset ID
RESOURCE_REGEX = re.compile(r"\.xlsx?$", re.I)     # pick the XLSX file

def latest_download_url(dataset_id: str) -> str:
    api = f"https://data.humdata.org/api/3/action/package_show?id={dataset_id}"
    meta = requests.get(api, timeout=30).json()
    if not meta["success"]:
        raise RuntimeError("HDX API error")
    for res in meta["result"]["resources"]:
        if RESOURCE_REGEX.search(res["name"]):
            return res["download_url"]
    raise RuntimeError("XLSX resource not found")

HDX_XLSX = latest_download_url(DATASET)
print("⬇️  Downloading", HDX_XLSX.split('/')[-1])
