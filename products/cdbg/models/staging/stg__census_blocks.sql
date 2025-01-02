{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['wkb_geometry'], 'type': 'gist'},
      {'columns': ['geoid']},
    ]
) }}
SELECT
    left(geoid, 12) AS block_group_geoid,
    *
FROM {{ source("recipe_sources", "dcp_cb2020_wi") }}
