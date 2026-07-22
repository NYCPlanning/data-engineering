{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

SELECT
    d.cb AS "CB2020",
    d.borocode AS "BoroCode",
    b.boroname AS "BoroName",
    d.ct AS "CT2020",
    d.bctcb AS "BCTCB2020",
    '36' || b.fips || d.ct || d.cb AS "GEOID",
    d.geom,
    st_perimeter(d.geom) AS "SHAPE_Length",
    st_area(d.geom) AS "SHAPE_Area"
FROM {{ ref('stg__censusblock2020') }} AS d
INNER JOIN {{ ref('stg__borough') }} AS b ON d.borocode = b.borocode
