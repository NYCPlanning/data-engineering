{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

SELECT
    d.bidid::int AS "BIDID",
    d.bid AS "BID",
    d.borough AS "BOROUGH",
    d.geom,
    st_perimeter(d.geom) AS "SHAPE_Length",
    st_area(d.geom) AS "SHAPE_Area"
FROM {{ ref('stg__businessimprovementdistrict') }} AS d
