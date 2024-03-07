SELECT
    variable_type,
    variable_id,
    buffer
FROM {{ ref('int__industrial_sources') }}
