{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['geom'], 'type': 'gist'},
      {'columns': ['geoid']},
    ]
) }}
SELECT
    geoid,
    left(geoid, 12) AS block_group_geoid,
    borocode::integer AS borough_code,
    boroname AS borough_name,
    ct2020,
    wkb_geometry AS geom
FROM {{ source("recipe_sources", "dcp_cb2020_wi") }}
