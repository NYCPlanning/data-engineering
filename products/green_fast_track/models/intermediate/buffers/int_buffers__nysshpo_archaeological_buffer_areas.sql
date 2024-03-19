WITH clipped_to_nyc AS (
    {{ clip_to_geom(source('recipe_sources', 'nysshpo_archaeological_buffer_areas'), left_by='wkb_geometry') }} -- noqa
)

-- We have multiple rows of archaeological areas, but none have an identifier
-- so it makes sense to treat them as a single polygon
SELECT
    'archaeological_buffer_area' AS variable_id,
    'archaeological_buffer_area' AS variable_type,
    st_union(geom) AS raw_geom,
    st_union(geom) AS buffer
FROM clipped_to_nyc
