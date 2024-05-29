SELECT
    variable_type,
    variable_id,
    lot_geom
FROM {{ ref("int_spatial__state_facility") }}
