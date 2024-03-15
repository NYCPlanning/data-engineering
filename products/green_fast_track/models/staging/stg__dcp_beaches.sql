SELECT
    'beaches' AS variable_type,
    agency || '-' || name AS variable_id,
    st_union(wkb_geometry) AS raw_geom,
    NULL AS buffer
FROM {{ source("recipe_sources", "dcp_beaches") }}
GROUP BY agency || '-' || name
