# scratch/map_roads.py
import geopandas as gpd, folium
from sqlalchemy import create_engine

engine = create_engine("postgresql://postgres:postgres@db:5432/pathfinder")

def main():
    roads = gpd.read_postgis(
        "SELECT geom, highway FROM sudan_roads_osm WHERE highway = 'primary' LIMIT 1000",
        con=engine,
        geom_col="geom",
        crs=4326,
    )

    m = folium.Map(location=[15, 30], zoom_start=5)
    folium.GeoJson(roads).add_to(m)
    m.save("/home/jovyan/work/maps/primary_roads.html")
    print("✅  Map written to maps/primary_roads.html")


if __name__ == "__main__":
    main()
