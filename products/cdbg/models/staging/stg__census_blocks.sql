{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['geom'], 'type': 'gist'},
      {'columns': ['geoid']},
    ]
) }}
SELECT
    geoid,
    borocode::integer AS borough_code,
    boroname AS borough_name,
    left(bctcb2020, 7) AS bct2020,
    ct2020,
    left(bctcb2020, 8) AS bctbg2020,
    left(geoid, 12) AS block_group_geoid,
    wkb_geometry AS geom
FROM {{ source("recipe_sources", "dcp_cb2020_wi") }}
