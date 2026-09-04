SELECT
    'snwa' AS variable_type,
    name AS variable_id,
    {{ dcp_geom_column('dcp_wrp_special_natural_waterfront_areas') }} AS raw_geom,
    NULL AS buffer
FROM {{ source("recipe_sources", "dcp_wrp_special_natural_waterfront_areas") }}
