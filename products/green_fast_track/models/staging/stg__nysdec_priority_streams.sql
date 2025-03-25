WITH clipped_to_nyc AS (
    {{ clip_to_geom(left=source("recipe_sources", "nysdec_priority_streams"), left_by="wkb_geometry") }}
)

SELECT
    'priority_waterbodies_streams' AS variable_type,
    seg_id || '-' || waterbody AS variable_id,
    geom AS raw_geom,
    NULL AS buffer
FROM clipped_to_nyc
