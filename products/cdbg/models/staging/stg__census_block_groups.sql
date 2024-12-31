{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['geom'], 'type': 'gist'},
      {'columns': ['geoid']},
    ]
) }}

WITH census_blocks AS (
    SELECT
        left(geoid, 12) AS block_group_geoid,
        *
    FROM {{ source("recipe_sources", "dcp_cb2020_wi") }}
)
SELECT
    borocode,
    boroname,
    ct2020,
    block_group_geoid AS geoid,
    st_union(wkb_geometry) AS geom
FROM census_blocks
GROUP BY
    borocode,
    boroname,
    ct2020,
    block_group_geoid
