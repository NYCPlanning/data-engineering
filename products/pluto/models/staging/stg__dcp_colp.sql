{{
  config(
    materialized='table',
    indexes=[
      {'columns': ['geom'], 'type': 'gist'}
    ]
  )
}}

SELECT
    *
FROM {{ source('recipe_sources', 'dcp_colp') }}
