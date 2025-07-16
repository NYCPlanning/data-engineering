SELECT
    variable_type,
    variable_id,
    raw_geom AS geom
FROM {{ ref("int_spatial__shadow_open_spaces") }}
WHERE st_geometrytype(raw_geom) = 'ST_MultiPolygon' AND lot_geom IS NULL
UNION ALL
SELECT
    variable_type,
    variable_id,
    lot_geom AS geom
FROM {{ ref("int_spatial__shadow_open_spaces") }}
WHERE lot_geom IS NOT NULL
