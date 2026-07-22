{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

SELECT
    d.hurricane_evacuation_zone AS "HURRICANE_EVACUATION_ZONE",
    d.geom,
    st_perimeter(d.geom) AS "SHAPE_Length",
    st_area(d.geom) AS "SHAPE_Area"
FROM {{ ref('stg__hurricaneevacuationzone') }} AS d
WHERE d.hurricane_evacuation_zone IS NOT NULL
