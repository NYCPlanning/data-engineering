WITH clipped_to_nyc AS (
    {{ clip_to_geom(left=source("recipe_sources", "nysdec_natural_heritage_communities"), left_by="wkb_geometry") }}
)

SELECT
    'natural_heritage_communities' AS variable_type,
    commonname AS variable_id,
    st_union(geom) AS raw_geom,
    NULL AS buffer
FROM clipped_to_nyc
GROUP BY commonname
