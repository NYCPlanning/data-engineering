WITH lot_block_groups AS (
    SELECT * FROM {{ ref("int__lot_block_groups") }}
),

pluto AS (
    SELECT
        bbl,
        bldgarea,
        resarea
    FROM {{ source("recipe_sources", "dcp_mappluto_clipped") }}
),

details AS (
    SELECT
        pluto.bbl,
        lot_block_groups.block_group_geoid,
        pluto.bldgarea,
        pluto.resarea,
        lot_block_groups.overlap_ratio
    FROM lot_block_groups
    LEFT JOIN pluto
        ON lot_block_groups.bbl = pluto.bbl
),

ratio_details AS (
    SELECT
        bbl,
        block_group_geoid,
        overlap_ratio,
        bldgarea,
        bldgarea * overlap_ratio AS bldgarea_in_block_group,
        resarea,
        resarea * overlap_ratio AS resarea_in_block_group
    FROM details
)

SELECT * FROM ratio_details
