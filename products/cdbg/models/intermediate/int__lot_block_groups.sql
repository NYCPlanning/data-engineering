WITH lot_block_groups AS (
    SELECT
        bbl,
        block_group_geoid,
        overlap_ratio
    FROM {{ ref("int__lot_block_groups_raw") }}
),

valid_lot_block_groups AS (
    SELECT * FROM lot_block_groups
    WHERE overlap_ratio IS NOT NULL
),

lots_easy AS (
    SELECT
        bbl,
        block_group_geoid,
        1 AS overlap_ratio
    FROM valid_lot_block_groups
    WHERE overlap_ratio > 0.9
),

lots_split AS (
    SELECT *
    FROM valid_lot_block_groups
    WHERE bbl NOT IN (SELECT bbl FROM lots_easy)
),

lots AS (
    SELECT * FROM lots_easy
    UNION ALL
    SELECT * FROM lots_split
)

SELECT * FROM lots
