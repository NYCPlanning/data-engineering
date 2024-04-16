SELECT
    variable_type,
    variable_id,
    raw_geom AS geom
FROM {{ ref('stg__nysdec_freshwater_wetlands') }}
