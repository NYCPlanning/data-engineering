SELECT
    'rec' AS variable_type,
    site_name AS variable_id,
    {{ dcp_geom_column('dcp_wrp_recognized_ecological_complexes') }} AS raw_geom,
    NULL AS buffer
FROM {{ source("recipe_sources", "dcp_wrp_recognized_ecological_complexes") }}
