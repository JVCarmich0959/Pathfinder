# scratch/map_roads.py
import geopandas as gpd, folium
from sqlalchemy import create_engine

engine = create_engine("postgresql://postgres:postgres@db:5432/pathfinder")

roads = gpd.read_postgis(
    "SELECT geometry, highway FROM sudan_roads_osm "
    "WHERE highway = 'primary' LIMIT 1000;",
    "CREATE OR REPLACE VIEW v_sudan_roads AS",
    "SELECT geometry AS geom, highway "
    "FROM   sudan_roads_osm;",
    con=engine,
    geom_col="geometry",   # <-- match the real column name
    crs=4326
)

m = folium.Map(location=[15, 30], zoom_start=5)
folium.GeoJson(roads).add_to(m)
m.save("/home/jovyan/work/maps/primary_roads.html")
print("✅  Map written to maps/primary_roads.html")
