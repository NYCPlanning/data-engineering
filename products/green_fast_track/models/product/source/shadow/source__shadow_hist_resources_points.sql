SELECT
    variable_type,
    variable_id,
    raw_geom
FROM {{ ref("int_spatial__shadow_hist_resources") }}
WHERE ST_GeometryType(raw_geom) = 'ST_MultiPoint' AND lot_geom IS NULL
