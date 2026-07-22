{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['geom'], 'type': 'gist'},
    ]
) }}

-- Water polygons, subdivided and indexed, used to clip the district layers to the
-- shoreline by subtraction.
--
-- Clipping to land is done by subtracting water rather than intersecting land: the
-- two are exact complements (4.632e9 + 8.423e9 = 13.055e9 sq ft) but water is 1,875
-- atomic polygons against land's 66,717, so it is far less geometry to process.
-- Dissolving land into a single mask instead produced one 7.2M-vertex, 117MB
-- geometry that took ~86 minutes to build and exhausted the build server's memory
-- when every district feature was intersected against it.
--
-- ST_Subdivide caps each row at 256 vertices so the GIST index stays selective and
-- each per-feature union touches only nearby pieces.
--
-- WATER_FLAG '1' is water; '2' (non-water), '3' (pier) and '4' are land. The ETL
-- spec predates flag '4' — see the note in the README.

SELECT st_subdivide(geom, 256) AS geom
FROM {{ ref('stg__atomicpolygons') }}
WHERE water_flag = '1'
