SELECT
    *,
    geom AS wkb_geometry
FROM {{ source('recipe_sources', 'dcp_mappluto_wi') }}
