{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['geom'], 'type': 'gist'},
      {'columns': ['geoid']},
    ]
) }}

WITH census_blocks AS (
    SELECT * FROM {{ ref("stg__census_blocks") }}
)

SELECT
    block_group_geoid AS geoid,
    bctbg2020,
    borough_code,
    borough_name,
    bct2020,
    ct2020,
    ST_Union(geom) AS geom
FROM census_blocks
GROUP BY
    block_group_geoid,
    bctbg2020,
    borough_code,
    borough_name,
    bct2020,
    ct2020
