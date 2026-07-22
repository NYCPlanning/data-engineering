{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

-- ElectDist packs the assembly district into the high digits (AD 23, ED 003 -> 23003).

WITH clipped AS (
    SELECT
        (d.assembly_district::int * 1000 + d.electdist::int) AS "ElectDist",
        {{ clipped_geom('d.geom') }} AS geom
    FROM {{ ref('stg__electiondistrict') }} AS d
    {{ clip_to_shoreline('d.geom') }}
)

SELECT
    *,
    st_perimeter(geom) AS "SHAPE_Length",
    st_area(geom) AS "SHAPE_Area"
FROM clipped
WHERE NOT st_isempty(geom)
