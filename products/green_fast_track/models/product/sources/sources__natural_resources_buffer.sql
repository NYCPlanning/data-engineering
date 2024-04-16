SELECT
    variable_type,
    variable_id,
    buffer AS geom
FROM {{ ref('int_buffers__natural_resource_shadows') }}
