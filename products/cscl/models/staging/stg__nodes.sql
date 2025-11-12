{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['nodeid']},
      {'columns': ['geom'], 'type': 'gist'},
    ]
) }}

SELECT * FROM {{ source("recipe_sources", "dcp_cscl_nodes") }}
