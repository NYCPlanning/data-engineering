SELECT * FROM {{ ref('int_vision_zero_priority_corridor_spine') }}
ORDER BY priority_corridor_id
