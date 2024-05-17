SELECT
    variable_type,
    variable_id,
    ST_UNION(buffer) AS geom
FROM {{ ref('int_buffers__historic_resource_shadows') }}
GROUP BY variable_type, variable_id
