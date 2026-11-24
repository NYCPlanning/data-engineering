{{ config(materialized='table') }}

SELECT
    name AS library_name,
    address,
    geom AS wkb_geometry
FROM {{ source('tdb_sources', 'qpl_libraries') }}