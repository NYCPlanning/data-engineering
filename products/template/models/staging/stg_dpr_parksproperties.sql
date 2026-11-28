{{ config(materialized='table') }}

SELECT
    signname AS space_name,
    borough,
    typecategory,
    wkb_geometry
FROM {{ source('tdb_sources', 'dpr_parksproperties') }}
