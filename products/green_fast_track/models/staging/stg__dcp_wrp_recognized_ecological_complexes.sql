SELECT
    'rec' AS variable_type,
    site_name AS variable_id,
    wkb_geometry AS raw_geom,
    NULL AS buffer
FROM {{ source("recipe_sources", "dcp_wrp_recognized_ecological_complexes") }}
