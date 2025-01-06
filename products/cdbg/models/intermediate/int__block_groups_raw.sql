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
    borough_code,
    borough_name,
    ct2020,
    st_union(geom) AS geom
FROM census_blocks
GROUP BY
    block_group_geoid,
    borough_code,
    borough_name,
    ct2020
