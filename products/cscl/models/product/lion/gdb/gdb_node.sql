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
SELECT
    nodeid::int AS "NODEID",
    globalid AS "GLOBALID",
    -- TODO: VIntersect ('' | 'VirtualIntersection') is not present in the source Node
    -- layer (stg__nodes has no virtual-intersection flag; master_flag and saftype don't
    -- correspond). The legacy ETL derived it elsewhere. Stubbed NULL until the source is
    -- identified. Prod distribution: 2 distinct values, dominated by ''.
    NULL::text AS "VIntersect",
    geom
FROM {{ ref('stg__nodes') }}
