SELECT
    variable_type,
    variable_id,
    lot_geom
FROM {{ ref("int_spatial__title_v_permit") }}
WHERE lot_geom IS NOT NULL
