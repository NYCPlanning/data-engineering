SELECT
    variable_type,
    variable_id,
    raw_geom
FROM {{ ref('int_buffers__dep_cats_permits') }}
WHERE ST_GEOMETRYTYPE(raw_geom) = 'ST_MultiPolygon'
