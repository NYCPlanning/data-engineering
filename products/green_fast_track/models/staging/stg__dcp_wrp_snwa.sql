SELECT
    'snwa' AS variable_type,
    name AS variable_id,
    shape AS raw_geom,
    NULL AS buffer
FROM {{ source("recipe_sources", "dcp_wrp_snwa") }}
