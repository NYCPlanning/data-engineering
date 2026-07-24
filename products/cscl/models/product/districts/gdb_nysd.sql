{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

WITH clipped AS (
    SELECT
        d.schooldist::int AS "SchoolDist",
        {{ clipped_geom('d.geom') }} AS geom
    FROM {{ ref('stg__schooldistrict') }} AS d
    {{ clip_to_shoreline('d.geom') }}
    WHERE d.schooldist::int != 10
),

-- District 10 spans two boroughs and is written out once per borough, both parts
-- keeping the same district number (ETL spec ch. 10, table 39 note **).
district_10_split AS (
    SELECT
        d.schooldist::int AS "SchoolDist",
        {{ clipped_geom('st_intersection(d.geom, b.geom)') }} AS geom
    FROM {{ ref('stg__schooldistrict') }} AS d
    INNER JOIN {{ ref('stg__borough') }} AS b ON st_intersects(d.geom, b.geom)
    {{ clip_to_shoreline('st_intersection(d.geom, b.geom)') }}
    WHERE d.schooldist::int = 10
)

SELECT
    *,
    st_perimeter(geom) AS "SHAPE_Length",
    st_area(geom) AS "SHAPE_Area"
FROM clipped
WHERE NOT st_isempty(geom)

UNION ALL

SELECT
    *,
    st_perimeter(geom) AS "SHAPE_Length",
    st_area(geom) AS "SHAPE_Area"
FROM district_10_split
WHERE NOT st_isempty(geom)
