SELECT * FROM {{ ref('int_open_space_walk_segment_spine') }}
ORDER BY segment_id
