SELECT
    'snwa' AS variable_type,
    name AS variable_id,
    wkb_geometry AS raw_geom,
    NULL AS buffer
FROM {{ source("recipe_sources", "dcp_wrp_special_natural_waterfront_areas") }}
