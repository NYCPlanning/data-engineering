SELECT
    variable_type,
    variable_id,
    raw_geom
FROM {{ ref('stg__lpc_scenic_landmarks') }}
