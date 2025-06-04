/* -----------------------------------------------------------
   01_normalize_road_layers.sql
   Standardises sudan_roads_osm imported from HOT-OSM
   ----------------------------------------------------------- */

-- 1) guarantee PostGIS
CREATE EXTENSION IF NOT EXISTS postgis;

-- 2) primary key if missing
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE  table_name = 'sudan_roads_osm' AND column_name = 'id'
    ) THEN
        ALTER TABLE sudan_roads_osm ADD COLUMN id bigserial PRIMARY KEY;
    END IF;
END $$;

-- Drop dependent materialized view to allow column type change
DROP MATERIALIZED VIEW IF EXISTS events_near_primary_roads;

-- 3) set SRID and correct type
ALTER TABLE sudan_roads_osm
  ALTER COLUMN geom
  TYPE geometry(LineString,4326)
  USING ST_SetSRID(geom,4326);

-- 4) indexes
CREATE INDEX IF NOT EXISTS idx_sudan_roads_geom
    ON sudan_roads_osm USING GIST (geom);

CREATE INDEX IF NOT EXISTS idx_sudan_roads_highway
    ON sudan_roads_osm (highway);

-- 5) optional lightweight view for maps
DROP MATERIALIZED VIEW IF EXISTS sudan_roads_osm_simplified;
CREATE MATERIALIZED VIEW sudan_roads_osm_simplified AS
SELECT id,
       highway,
       ST_SimplifyPreserveTopology(geom, 0.0005) AS geom
FROM   sudan_roads_osm;

CREATE INDEX IF NOT EXISTS idx_sudan_roads_simpl_geom
    ON sudan_roads_osm_simplified USING GIST (geom);

-- 6) planner stats
ANALYZE sudan_roads_osm;
ANALYZE sudan_roads_osm_simplified;

-- Recreate dependent view after column alteration
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
