SELECT
    variable_id,
    raw_geom
FROM {{ ref("int_spatial__shadow_hist_resources") }}
WHERE ST_GEOMETRYTYPE(raw_geom) = 'ST_MultiPoint'
