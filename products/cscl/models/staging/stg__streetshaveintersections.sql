{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['nodeid']},
      {'columns': ['segmentid']},
    ]
) }}

SELECT * FROM {{ source("recipe_sources", "dcp_cscl_streetshaveintersections") }}
