-- TODO: implement node point layer for LION GDB
-- Schema: OBJECTID (int), NODEID (int), GLOBALID (str), VIntersect (str), geometry (Point, EPSG:2263)
-- Source: stg__nodes (has nodeid, geom)
{{ config(materialized='table') }}
SELECT 1 AS stub
WHERE false
