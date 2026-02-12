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
FROM {{ source('recipe_sources', 'dcp_cb2010_wi') }}
