SELECT * FROM {{ ref('int_vision_zero_priority_intersection_spine') }}
ORDER BY priority_intersection_id
