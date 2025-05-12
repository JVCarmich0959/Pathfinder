# Pathfinder

Risk-aware, offline-friendly maps and routing tools for Sudan–Chad
humanitarian operations. Pathfinder fuses fresh satellite imagery,
OpenStreetMap edits, and conflict-event data to publish daily safe-route
layers (GeoJSON, MBTiles, printable PDFs) for refugees, local drivers,
and aid convoys.

---

## Quick start (tech)

```bash
# clone & spin up complete stack
git clone https://github.com/JVCarmichael0959/pathfinder.git
cd pathfinder
docker compose up      # PostGIS + pgRouting + Jupyter + Flask
