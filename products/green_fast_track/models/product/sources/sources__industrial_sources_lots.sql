SELECT
    variable_type,
    variable_id,
    raw_geom
FROM {{ ref('int__industrial_sources') }}
