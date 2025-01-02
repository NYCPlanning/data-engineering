{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['geom'], 'type': 'gist'},
      {'columns': ['geoid']},
    ]
) }}

WITH census_blocks AS (
    SELECT * FROM {{ ref("stg__census_blocks") }}
),

census_data_blocks AS (
    SELECT * FROM {{ ref("stg__census_data_blocks") }}
),

blocks AS (
    SELECT
        census_blocks.block_group_geoid,
        census_data_blocks.borough_code,
        census_data_blocks.borough_name,
        census_blocks.ct2020,
        census_data_blocks.total_population,
        census_blocks.wkb_geometry
    FROM census_blocks
    LEFT JOIN census_data_blocks
        ON census_blocks.bctcb2020 = census_data_blocks.bctcb2020
)

SELECT
    block_group_geoid AS geoid,
    borough_code,
    borough_name,
    ct2020,
    sum(total_population) AS total_population,
    st_union(wkb_geometry) AS geom
FROM blocks
GROUP BY
    block_group_geoid,
    borough_code,
    borough_name,
    ct2020
