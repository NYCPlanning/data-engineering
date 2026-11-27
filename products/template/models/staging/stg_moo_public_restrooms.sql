{{ config(materialized='table') }}

SELECT
    facility_name AS restroom_name,
    location_type,
    operator,
    status,
    hours_of_operation,
    accessibility,
    geom AS wkb_geometry
FROM {{ source('tdb_sources', 'moo_public_restrooms') }}
