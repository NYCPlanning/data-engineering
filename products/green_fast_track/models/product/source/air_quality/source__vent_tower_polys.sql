SELECT
    variable_type,
    variable_id,
    raw_geom
FROM {{ ref("int_spatial__vent_tower") }}
