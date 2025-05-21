-- -------------------------------------------------------------
-- 03_geo_join.sql  –  Events near primary roads (5 km buffer)
-- Creates a materialized view linking conflict events to nearby
-- primary road segments from the HDX OSM export.
-- -------------------------------------------------------------

CREATE EXTENSION IF NOT EXISTS postgis;

DROP MATERIALIZED VIEW IF EXISTS events_near_primary_roads;
CREATE MATERIALIZED VIEW events_near_primary_roads AS
SELECT r.id       AS road_id,
       e.event_id AS event_id,
       e.event_date,
       ST_Distance(e.geom::geography, r.geom::geography) AS distance_m,
       e.geom AS event_geom,
       r.geom AS road_geom
FROM   events_raw e
JOIN   sudan_roads_osm r
       ON r.highway = 'primary'
      AND ST_DWithin(e.geom::geography, r.geom::geography, 5000);

CREATE INDEX IF NOT EXISTS events_near_primary_roads_geom_idx
    ON events_near_primary_roads USING GIST (event_geom);
