SELECT
    variable_id,
    raw_geom
FROM {{ ref("int_spatial__title_v_permit") }}
