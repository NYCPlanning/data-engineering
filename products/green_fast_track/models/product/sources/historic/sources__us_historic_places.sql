SELECT
    variable_type,
    variable_id,
    raw_geom
FROM {{ ref('int_buffers__nysparks_historicplaces') }}
