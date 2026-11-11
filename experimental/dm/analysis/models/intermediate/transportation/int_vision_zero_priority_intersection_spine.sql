SELECT
    street_1 || ' and ' || street_2 AS priority_intersection_id,
    street_1,
    street_2,
    geom
FROM {{ ref('stg_dot__vzv_priority_intersections') }}
