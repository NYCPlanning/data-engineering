SELECT
    'beaches' AS variable_type,
    agency || '-' || name AS variable_id,
    st_union_agg({{ dcp_geom_column('dcp_beaches') }}) AS raw_geom,
    NULL AS buffer
FROM {{ source("recipe_sources", "dcp_beaches") }}
GROUP BY agency || '-' || name
