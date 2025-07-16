{{ config(
    materialized = 'table',
    indexes=[
        {'columns': ['geom'], 'type': 'gist'},
    ]
) }}

WITH polygons AS (
    -- this is unfortunately all necessary
    -- - union to merge boros. this now has very narrow, line-like holes between boros in parts
    -- - dump then dumps this multipolygon (with holes represented) to individual polygons, some of them being the "holes"
    -- - ExteriorRing then draws exterior ring around these, giving us three lines - one for mainland (wi) NYC, one each for ellis and liberty islands
    -- - MakePolygon converts these lines to polygons
    SELECT
        st_makepolygon(
            st_exteriorring((st_dump(st_union(wkb_geometry))).geom)
        ) AS geom
    FROM {{ source('recipe_sources', 'dcp_boroboundaries_wi') }}
)

-- another union is done to get a single row with all NYC area in a multipolygon
-- postgresql is unhappy if this is attempted in the previous query
SELECT st_transform(st_union(geom), 2263) AS geom
FROM polygons
