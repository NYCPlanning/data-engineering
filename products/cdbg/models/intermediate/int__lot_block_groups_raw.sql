WITH pluto AS (
    SELECT
        bbl,
        wkb_geometry
    FROM {{ source("recipe_sources", "dcp_mappluto_clipped") }}
),

block_groups AS (
    SELECT
        geoid,
        geom
    FROM {{ ref("int__block_groups_raw") }}
),

lot_block_group_intersections AS (
    SELECT
        pluto.bbl,
        pluto.wkb_geometry AS lot_geometry,
        ST_Area(pluto.wkb_geometry) AS lot_area_sqft,
        block_groups.geoid AS block_group_geoid,
        block_groups.geom AS block_group_geometry
    FROM pluto
    LEFT JOIN block_groups
        ON ST_Intersects(pluto.wkb_geometry, block_groups.geom)
),

intersection_calculations AS (
    SELECT
        bbl,
        lot_geometry,
        lot_area_sqft,
        block_group_geoid,
        block_group_geometry,
        ST_Area(
            CASE
                WHEN ST_CoveredBy(lot_geometry, block_group_geometry) THEN lot_geometry
                ELSE ST_Intersection(lot_geometry, block_group_geometry)
            END
        ) AS area_of_intersection_sqft
    FROM lot_block_group_intersections
),

intersection_ratios AS (
    SELECT
        bbl,
        block_group_geoid,
        area_of_intersection_sqft / lot_area_sqft AS overlap_ratio
    FROM intersection_calculations
)

SELECT * FROM intersection_ratios
