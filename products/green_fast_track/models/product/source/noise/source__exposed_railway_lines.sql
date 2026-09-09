SELECT
    variable_type,
    variable_id,
    raw_geom
FROM {{ ref("int_spatial__exposed_railway") }}
WHERE ST_GEOMETRYTYPE(raw_geom) = 'MULTILINESTRING'
