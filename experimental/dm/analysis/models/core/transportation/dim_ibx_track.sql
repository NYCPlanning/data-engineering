SELECT * FROM {{ ref('int_ibx_track_spine') }}
ORDER BY ibx_track_id
