{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

-- PUMAs are not modelled in CSCL; they are dissolved from 2010 census tracts.

WITH dissolved AS (
    SELECT
        puma,
        -- gridSize=0.001: a plain st_union over many source-tract polygons meeting at
        -- exactly-coincident vertices can still produce spurious needle-shaped holes
        -- (a GEOS overlay-robustness artifact, not a real gap) - see
        -- int__water_mask.sql's comment and chat log 2026-10-03.
        st_union(geom, 0.001) AS geom
    FROM {{ ref('stg__censustract2010') }}
    WHERE puma IS NOT NULL
    GROUP BY puma
),

clipped AS (
    SELECT
        d.puma AS "PUMA",
        {{ clipped_geom('d.geom') }} AS geom
    FROM dissolved AS d
    {{ clip_to_shoreline('d.geom') }}
)

SELECT
    *,
    st_perimeter(geom) AS "SHAPE_Length",
    st_area(geom) AS "SHAPE_Area"
FROM clipped
WHERE NOT st_isempty(geom)
