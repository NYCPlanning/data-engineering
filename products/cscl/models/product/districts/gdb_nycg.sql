{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

WITH clipped AS (
    SELECT
        d.congdist::int AS "CongDist",
        {{ clipped_geom('d.geom') }} AS geom
    FROM {{ ref('stg__congressionaldistrict') }} AS d
    {{ clip_to_shoreline('d.geom') }}
)

SELECT
    *,
    st_perimeter(geom) AS "SHAPE_Length",
    st_area(geom) AS "SHAPE_Area"
FROM clipped
WHERE NOT st_isempty(geom)
