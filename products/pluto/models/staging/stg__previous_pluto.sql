{{
  config(
    materialized='table',
    indexes=[
      {'columns': ['geom'], 'type': 'gist'}
    ]
  )
}}

-- Previous version of PLUTO for change detection
SELECT
    *,
    wkb_geometry AS geom
FROM {{ source('build_sources', 'previous_pluto') }}
