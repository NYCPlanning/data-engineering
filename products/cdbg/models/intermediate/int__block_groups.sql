WITH lot_block_groups AS (
    SELECT * FROM {{ ref("int__lot_block_groups_details") }}
),

block_groups AS (
    SELECT * FROM {{ ref("int__block_groups_raw") }}
),

block_groups_income AS (
    SELECT * FROM {{ ref("stg__low_mod_by_block_group") }}
),

block_groups_demographics AS (
    SELECT
        block_groups.geoid,
        block_groups.bctbg2020,
        block_groups.borough_code,
        block_groups.borough_name,
        block_groups.bct2020,
        block_groups.ct2020,
        block_groups.geom,
        block_groups_income.tract,
        block_groups_income.block_group,
        block_groups_income.potential_lowmod_population,
        block_groups_income.low_mod_income_population,
        block_groups_income.low_income_population,
        block_groups_income.mod_income_population,
        block_groups_income.low_mod_income_population_percentage
    FROM block_groups
    LEFT JOIN block_groups_income
        ON block_groups.geoid = block_groups_income.geoid
),

block_groups_floor_area AS (
    SELECT
        block_group_geoid AS geoid,
        sum(bldgarea_in_block_group) AS total_floor_area,
        sum(resarea_in_block_group) AS residential_floor_area
    FROM lot_block_groups
    GROUP BY geoid
),

block_group_details AS (
    SELECT
        block_groups_demographics.*,
        coalesce(block_groups_floor_area.total_floor_area, 0) AS total_floor_area,
        coalesce(block_groups_floor_area.residential_floor_area, 0) AS residential_floor_area
    FROM block_groups_demographics
    LEFT JOIN block_groups_floor_area
        ON block_groups_demographics.geoid = block_groups_floor_area.geoid
),

block_group_calculations AS (
    SELECT
        block_group_details.*,
        CASE
            WHEN total_floor_area = 0
                THEN 0
            ELSE (residential_floor_area / total_floor_area) * 100
        END AS residential_floor_area_percentage
    FROM block_group_details
)

SELECT * FROM block_group_calculations
