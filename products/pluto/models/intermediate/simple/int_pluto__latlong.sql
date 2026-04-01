{{
    config(
        materialized='table',
        indexes=[{'columns': ['bbl'], 'unique': True}],
        tags=['pluto_enrichment']
    )
}}

-- Calculate latitude and longitude from x/y coordinates
-- Transform from State Plane (SRID 2263) to WGS84 (SRID 4326)
-- Also create centroid geometry point
-- Use CTE to compute transform once and reuse to avoid floating-point drift

WITH transformed AS (
    SELECT
        bbl,
        ST_TRANSFORM(
            ST_SETSRID(
                ST_MAKEPOINT(xcoord::double precision, ycoord::double precision),
                2263
            ),
            4326
        ) AS point_4326
    FROM {{ target.schema }}.pluto
    WHERE xcoord IS NOT NULL
)

SELECT
    bbl,
    ST_Y(point_4326) AS latitude,
    ST_X(point_4326) AS longitude,
    ST_SETSRID(
        ST_MAKEPOINT(
            ST_X(point_4326)::double precision,
            ST_Y(point_4326)::double precision
        ),
        4326
    ) AS centroid
FROM transformed
