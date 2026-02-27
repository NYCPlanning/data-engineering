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
FROM {{ source('recipe_sources', 'previous_pluto') }}
