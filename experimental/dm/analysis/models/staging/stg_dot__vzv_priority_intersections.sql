-- DISTINCT drops the one exact duplicate row
SELECT DISTINCT
    street_1,
    street_2,
    geom
FROM {{ source('sources', 'vzv_priority_intersections') }}
