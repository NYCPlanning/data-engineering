-- district_boundary_build_raw (land only), split into its CTEs so each is timed.
-- PostGIS: st_dump(st_polygonize(geom)) GROUP BY entity_id (ST_Polygonize is an aggregate).
-- DuckDB spatial: ST_Polygonize is a scalar over GEOMETRY[], so list() the edges first;
-- ST_Dump returns a LIST of STRUCT(geom, path), unnested here.
CREATE OR REPLACE TABLE {name}_faces AS
SELECT entity_id, unnest(ST_Dump(ST_Polygonize(list(geom))), recursive := true)
FROM {name}_edges
WHERE edge_type = 'exterior'
GROUP BY entity_id;

CREATE OR REPLACE TABLE {name}_member_points AS
SELECT m.{entity_column} AS entity_id, ST_PointOnSurface(a.geom) AS pt
FROM ap_entities AS m
INNER JOIN atomicpolygons AS a ON m.atomicid = a.atomicid
WHERE m.{entity_column} IS NOT NULL AND m.water_flag != '1';

CREATE OR REPLACE TABLE {name}_kept AS
SELECT f.entity_id, f.geom AS face
FROM {name}_faces AS f
WHERE EXISTS (
    SELECT 1 FROM {name}_member_points AS mp
    WHERE mp.entity_id = f.entity_id AND ST_Contains(f.geom, mp.pt)
);

CREATE OR REPLACE TABLE {name}_raw AS
SELECT entity_id, {union_agg}(face) AS geom
FROM {name}_kept
GROUP BY entity_id;
