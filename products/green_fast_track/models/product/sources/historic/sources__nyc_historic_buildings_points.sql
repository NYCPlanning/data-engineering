SELECT
    variable_type,
    variable_id,
    raw_geom
FROM {{ ref('int_buffers__lpc_landmarks') }}
WHERE ST_GEOMETRYTYPE(raw_geom) = 'ST_MultiPoint'
