{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

SELECT
    d.bidid::int AS "BIDID",
    d.bid AS "BID",
    d.borough AS "BOROUGH",
    -- publish the source geometry as stored, like prod; the staging make_valid alters BIDID 56
    d.raw_geom AS geom,
    st_perimeter(d.raw_geom) AS "SHAPE_Length",
    st_area(d.raw_geom) AS "SHAPE_Area"
FROM {{ ref('stg__businessimprovementdistrict') }} AS d
