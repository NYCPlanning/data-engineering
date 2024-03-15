WITH clipped_to_nyc AS (
    {{ clip_to_geom(left=source("recipe_sources", "nysdec_priority_shorelines"), left_by="wkb_geometry") }}
)

SELECT
    'priority_waterbodies' AS variable_type,
    pwl_id || '-' || name AS variable_id,
    geom AS raw_geom,
    NULL AS buffer
FROM clipped_to_nyc
