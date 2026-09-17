{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['nodeid']},
    ]
) }}

SELECT * FROM {{ source("recipe_sources", "dcp_cscl_virtualintersection") }}
