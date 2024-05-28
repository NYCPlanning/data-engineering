SELECT
    variable_id,
    lot_geom
FROM {{ ref("int_spatial__title_v_permit") }}
