{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

-- ElectDist packs the assembly district into the high digits (AD 23, ED 003 -> 23003).

SELECT
    (d.assembly_district::int * 1000 + d.electdist::int) AS "ElectDist",
    d.geom,
    st_perimeter(d.geom) AS "SHAPE_Length",
    st_area(d.geom) AS "SHAPE_Area"
FROM {{ ref('stg__electiondistrict') }} AS d
