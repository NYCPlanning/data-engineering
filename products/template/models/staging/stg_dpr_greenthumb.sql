{{ config(materialized='table') }}

SELECT
    gardenname AS space_name,
    borough,
    wkb_geometry
FROM {{ source('tdb_sources', 'dpr_greenthumb') }}
