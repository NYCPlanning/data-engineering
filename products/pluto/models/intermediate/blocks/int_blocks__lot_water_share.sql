{{
    config(
        materialized='table',
        indexes=[{'columns': ['bbl'], 'unique': True}],
        tags=['blocks']
    )
}}

-- How much of each lot the shoreline clip removes. The lots themselves stay unclipped.

WITH areas AS (
    SELECT
        bbl,
        ST_AREA(geom_2263) AS lot_area,
        -- clamped, since the clipped area can come out a hair larger than the lot
        GREATEST(ST_AREA(geom_2263) - ST_AREA(clipped_2263), 0) AS water_area
    FROM {{ source('build_sources', 'pluto_geom') }}
    WHERE geom_2263 IS NOT NULL
)

SELECT
    bbl,
    lot_area,
    water_area,
    water_area / NULLIF(lot_area, 0) AS water_share,
    COALESCE(water_area / NULLIF(lot_area, 0) > 0.9, FALSE) AS is_mostly_water
FROM areas
