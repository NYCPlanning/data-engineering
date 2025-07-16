SELECT
    variable_type,
    variable_id,
    raw_geom
FROM {{ ref("int_spatial__natural_resources") }}
WHERE st_geometrytype(raw_geom) = 'ST_MultiPoint'
