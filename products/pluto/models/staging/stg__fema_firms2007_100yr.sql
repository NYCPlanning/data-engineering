{{
  config(
    materialized='table',
    indexes=[
      {'columns': ['geom'], 'type': 'gist'}
    ]
  )
}}

SELECT *
FROM {{ source('recipe_sources', 'fema_firms2007_100yr') }}
