SELECT
    *,
    fmsid AS fms_id,
    ST_TRANSFORM(wkb_geometry, 2263) AS geom
FROM {{ source('recipe_sources', 'dot_projects_bridges') }}
