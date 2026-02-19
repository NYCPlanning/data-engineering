{{
  config(
    materialized='table',
  )
}}

SELECT
    *
FROM {{ source('recipe_sources', 'pluto_pts') }}
