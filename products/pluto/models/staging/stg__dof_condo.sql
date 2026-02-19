{{
  config(
    materialized='table',
  )
}}

SELECT
    *
FROM {{ source('recipe_sources', 'dof_condo') }}
