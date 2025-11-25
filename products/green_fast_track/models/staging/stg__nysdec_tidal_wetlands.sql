WITH clipped_to_nyc AS (
    {{ clip_to_geom(left=source("recipe_sources", "nysdec_tidal_wetlands")) }}
)

SELECT
    'tidal_wetlands' AS variable_type,
    id::text AS variable_id,
    geom AS raw_geom,
    NULL AS buffer
FROM clipped_to_nyc
