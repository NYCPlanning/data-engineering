SELECT
    *,
    ST_TRANSFORM(geom, 2263) AS geom
FROM {{ source('recipe_sources', 'dcp_mappluto_wi') }}
