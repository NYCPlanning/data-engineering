{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

SELECT
    d.name AS "NAME",
    d.lp_number AS "LP_NUMBER",
    d.designated AS "DESIGNATED",
    d.borough AS "BOROUGH",
    d.extension AS "EXTENSION",
    d.created_by AS "CREATED_BY",
    d.created_date AS "CREATED_DATE",
    d.modified_by AS "MODIFIED_BY",
    d.modified_date AS "MODIFIED_DATE",
    d.geom,
    st_perimeter(d.geom) AS "SHAPE_Length",
    st_area(d.geom) AS "SHAPE_Area"
FROM {{ ref('stg__historicdistrict') }} AS d
