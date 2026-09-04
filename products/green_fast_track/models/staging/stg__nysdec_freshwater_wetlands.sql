SELECT
    'freshwater_wetlands' AS variable_type,
    wetid AS variable_id,
    {{ dcp_geom_column('nysdec_freshwater_wetlands') }} AS raw_geom,
    NULL AS buffer
FROM {{ source("recipe_sources", "nysdec_freshwater_wetlands") }}
WHERE name IN ('Bronx', 'Kings', 'Queens', 'Richmond')
