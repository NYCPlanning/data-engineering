{{ config(materialized='table') }}

SELECT
    name AS library_name,
    locality AS borough,
    address,
    wkb_geometry
FROM {{ source('tdb_sources', 'nypl_libraries') }}
