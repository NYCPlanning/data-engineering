WITH clipped_to_nyc AS (
    {{ clip_to_geom(source('recipe_sources', 'nysshpo_archaeological_buffer_areas'), left_by='wkb_geometry') }} -- noqa
)

-- We have multiple rows of archaeological areas, but none have an identifier
-- so it makes sense to treat them as a single polygon
SELECT
    'archaeological_area' AS flag_id_field_name,
    'archaeological_areas' AS variable_type,
    'Archaeological Areas' AS variable_id,
    ST_Union(geom) AS raw_geom,
    NULL AS buffer
FROM clipped_to_nyc
