# scratch/map_roads.py
import geopandas as gpd
import folium
from pathfinder.settings import engine

HIWAYS = ["primary"]

def main():
    sql = (
        "SELECT geom, highway FROM sudan_roads_osm "
        "WHERE highway = ANY(%(hiways)s::text[]) LIMIT 1000"
    )
    roads = gpd.read_postgis(
        sql,
        con=engine(),
        params={"hiways": HIWAYS},
        geom_col="geom",
        crs=4326,
    )

    m = folium.Map(location=[15, 30], zoom_start=5)
    folium.GeoJson(roads).add_to(m)
    m.save("/home/jovyan/work/maps/primary_roads.html")
    print("✅  Map written to maps/primary_roads.html")


if __name__ == "__main__":
    main()
