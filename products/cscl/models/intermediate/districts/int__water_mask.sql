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
--
-- Adjacent atomic polygons meant to share an edge exactly instead differ by
-- ~0.0001-0.0002 ft after import (confirmed empirically - see CSCL-DISTRICTS-03),
-- leaving hairline gaps between them. Left alone, wherever a district's own source
-- boundary happens to run parallel to a chain of these gaps, they accumulate into
-- a long, thin, un-clipped "spur" reaching into the water (up to ~4,000 ft on some
-- coastline-adjacent NTAs) once differenced against this mask. A small
-- buffer-union-debuffer ("morphological closing") merges water polygons across
-- gaps below 0.02 ft before subdividing, closing them without perceptibly changing
-- any real coastline shape - confirmed to match true ST_Snap's result on a known
-- spur, at a fraction of the cost (~19s citywide vs. ~70s+). quad_segs=2 keeps the
-- tiny buffer from bloating vertex count with circular-arc approximation it doesn't
-- need at this scale.
--
-- Buffer distance is 0.01 ft, not the larger 0.05 ft first tried: at 0.05 ft the
-- closing merged some water polygons across a real (if narrow) non-water feature
-- between them - confirmed on MN24, where it ate a genuine sliver of land prod
-- keeps (0.1 sq ft over a 3,230 ft perimeter - a thin real feature, not noise).
-- 0.01 ft is still ~50x the ~0.0001-0.0002 ft import-precision gap this is meant to
-- close, with much less room to reach a real feature by mistake.
--
-- Reverted off int__atomicpolygon_rebuilt's grid-snapped topology (see git history for
-- that version) - on the real citywide comparison it left the worst spurs (SI12,
-- QN98, QN45 - genuine coverage voids, not vertex misalignment - see
-- CSCL-DISTRICTS-03) completely untouched, while introducing the same MN24-style
-- regressions on other previously-exact NTAs. Back on stg__atomicpolygons directly
-- pending further investigation.

WITH closed AS (
    SELECT
        st_buffer(
            st_union(st_buffer(geom, 0.01, 'quad_segs=2')),
            -0.01, 'quad_segs=2'
        ) AS geom
    FROM {{ ref('stg__atomicpolygons') }}
    WHERE water_flag = '1'
)

SELECT st_subdivide(geom, 256) AS geom
FROM closed
