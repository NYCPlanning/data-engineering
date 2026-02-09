{{ config(materialized='table') }}

SELECT
    name AS library_name,
    geom AS wkb_geometry
FROM {{ source('tdb_sources', 'qpl_libraries') }}
