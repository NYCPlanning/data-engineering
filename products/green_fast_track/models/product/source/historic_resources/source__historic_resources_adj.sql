SELECT
    variable_id,
    raw_geom
FROM {{ ref("int_spatial__historic_resources_adj") }}
