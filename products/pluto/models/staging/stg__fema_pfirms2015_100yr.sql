{{
  config(
    materialized='table',
    indexes=[
      {'columns': ['geom'], 'type': 'gist'}
    ]
  )
}}

SELECT
    *,
    wkb_geometry AS geom
FROM {{ source('recipe_sources', 'fema_pfirms2015_100yr') }}
