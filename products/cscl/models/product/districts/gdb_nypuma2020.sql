{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

-- PUMAs are not modelled in CSCL; they are dissolved from 2020 census tracts.
-- Table 39 predates this feature class but prod ships it.

WITH dissolved AS (
    SELECT
        puma,
        st_union(geom) AS geom
    FROM {{ ref('stg__censustract2020') }}
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
