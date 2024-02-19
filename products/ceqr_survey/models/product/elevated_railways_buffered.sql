SELECT
    variable_type,
    variable_id,
    buffer
FROM {{ ref('int__elevated_railways') }}
