{{ config(materialized='table') }}

SELECT
    boroname AS borough,
    wkb_geometry
FROM {{ source('tdb_sources', 'dcp_boroboundaries_wi') }}
