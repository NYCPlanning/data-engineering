SELECT
    variable_type,
    variable_id,
    buffer AS geom
FROM {{ ref('int_buffers__historic_resource_shadows') }}
