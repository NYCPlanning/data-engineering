{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

SELECT
    d.bidid::int AS "BIDID",
    d.bid AS "BID",
    d.borough AS "BOROUGH",
    -- valid polygon (make_valid); prod publishes BID 56's invalid nested-shell geometry (GR question inv-3vi.18)
    d.geom,
    st_perimeter(d.geom) AS "SHAPE_Length",
    st_area(d.geom) AS "SHAPE_Area"
FROM {{ ref('stg__businessimprovementdistrict') }} AS d
