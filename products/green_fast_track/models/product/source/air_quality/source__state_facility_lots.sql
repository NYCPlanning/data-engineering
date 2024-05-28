SELECT
    variable_id,
    lot_geom
FROM {{ ref("int_spatial__state_facility") }}
