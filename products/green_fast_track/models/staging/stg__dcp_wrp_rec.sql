SELECT
    'rec' AS variable_type,
    site_name AS variable_id,
    shape AS raw_geom,
    NULL AS buffer
FROM {{ source("recipe_sources", "dcp_wrp_rec") }}
