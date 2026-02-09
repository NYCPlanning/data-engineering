{{ config(materialized='table') }}

SELECT
    name AS library_name,
    locality AS borough,
    wkb_geometry
FROM {{ source('tdb_sources', 'nypl_libraries') }}
