{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

-- Node point layer for the published LION GDB (nyclion_*.zip).
-- Source: the `Node` layer of the CSCL ETL Working GDB (loaded via GDAL as
-- `dcp_cscl_nodes`, surfaced by stg__nodes).
-- Prod schema: OBJECTID (int32), NODEID (int32), GLOBALID (str), VIntersect (str), Point/EPSG:2263.
--
-- OBJECTID is intentionally not emitted. It isn't in the source Node layer, and the
-- OpenFileGDB writer always consumes a column named OBJECTID as the (hidden) FID — so
-- our written layer already has an OBJECTID/FID; it just isn't a listed attribute the
-- way prod's ESRI-written copy redundantly stores it. (Same as gdb_lion, which also
-- doesn't list OBJECTID.) An emitted column would vanish into the FID with no effect.
--
-- VIntersect and the node exclusion below come from the legacy Create_Node_Shape(12B).py
-- script (J. Ding, ITD-DCP), not the ETL spec doc - a node is a "virtual intersection"
-- (VIntersect = 'VirtualIntersection') iff it's listed in VIRTUALINTERSECTION *and* has
-- at least one segment in STREETSHAVEINTERSECTIONS. If it's in VIRTUALINTERSECTION with
-- zero such segments, the legacy script deletes it from the output entirely (a
-- "floating" virtual node with nothing attached), rather than emitting it with a blank
-- flag - hence the WHERE clause below, not just the CASE. Everything else gets a blank
-- VIntersect (untouched by either source table).
WITH virtual_segment_counts AS (
    SELECT
        virtualintersection.nodeid,
        count(streetshaveintersections.segmentid) AS n_segments
    FROM {{ ref('stg__virtualintersection') }} AS virtualintersection
    LEFT JOIN {{ ref('stg__streetshaveintersections') }} AS streetshaveintersections
        ON virtualintersection.nodeid = streetshaveintersections.nodeid
    GROUP BY virtualintersection.nodeid
)

SELECT
    nodes.nodeid::int AS "NODEID",
    nodes.globalid AS "GLOBALID",
    CASE
        WHEN counts.n_segments > 0 THEN 'VirtualIntersection'
    END AS "VIntersect",
    nodes.geom
FROM {{ ref('stg__nodes') }} AS nodes
LEFT JOIN virtual_segment_counts AS counts ON nodes.nodeid = counts.nodeid
WHERE counts.n_segments IS DISTINCT FROM 0
