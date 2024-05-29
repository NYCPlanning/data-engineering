SELECT
    variable_type,
    variable_id,
    raw_geom
FROM {{ ref("int_spatial__shadow_open_spaces") }}
WHERE ST_GEOMETRYTYPE(raw_geom) = 'ST_MultiPolygon'
