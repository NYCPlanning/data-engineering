SELECT
    variable_id,
    raw_geom
FROM {{ ref("stg__nysdec_freshwater_wetlands_checkzones") }}
