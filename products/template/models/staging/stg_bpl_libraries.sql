{{ config(materialized='table') }}

SELECT
    title AS library_name,
    address,
    wkb_geometry
FROM {{ source('tdb_sources', 'bpl_libraries') }}