SELECT
    variable_type,
    variable_id,
    lot_geom
FROM {{ ref("int_spatial__cats_permits") }}
WHERE lot_geom IS NOT NULL
