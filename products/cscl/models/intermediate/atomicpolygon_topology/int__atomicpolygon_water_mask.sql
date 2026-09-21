{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['geom'], 'type': 'gist'},
    ]
) }}

-- POC comparison for int__water_mask.sql: same idea (union the water atomic polygons,
-- subdivide for a selective GIST index), but sourced from int__atomicpolygon_rebuilt
-- (node-snapped) instead of raw stg__atomicpolygons - no buffer-union-debuffer
-- morphological-closing trick needed if node-snapping alone already closes the
-- CSCL-DISTRICTS-03 hairline gaps (measured at ~0.00005-0.00017 ft between adjacent
-- water atomic polygons - two to three orders of magnitude below the 0.02ft grid
-- tolerance int__atomicpolygon_nodes was calibrated to, so well inside "definitely the
-- same point" territory). See products/cscl chat log, 2026-09-20, for the MN31/SI12/
-- QN98/QN45 spur measurements this is being tested against.
WITH unioned AS (
    SELECT st_unaryunion(st_collect(r.geom)) AS geom
    FROM {{ ref('int__atomicpolygon_rebuilt') }} AS r
    INNER JOIN {{ ref('stg__atomicpolygons') }} AS a ON a.atomicid = r.atomicid
    WHERE a.water_flag = '1'
)

SELECT st_subdivide(geom, 256) AS geom
FROM unioned
