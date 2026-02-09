{{ config(materialized='table') }}

SELECT
    signname AS space_name,
    borough,
    wkb_geometry
FROM {{ source('tdb_sources', 'dpr_parksproperties') }}
