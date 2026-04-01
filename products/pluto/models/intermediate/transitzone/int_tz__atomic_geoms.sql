{{
    config(
        materialized='table',
        indexes=[{'columns': ['wkb_geometry'], 'type': 'gist'}],
        tags=['pluto_enrichment']
    )
}}

-- Decompose transit zone multipolygons into atomic parts for performance
-- Breaking apart multipolygons reduces spatial calculation time from >10 min to ~1 min

WITH decomposed AS (
    SELECT
        transit_zone,
        (ST_DUMP(geom)).geom AS wkb_geometry
    FROM {{ ref('stg__dcp_transit_zones') }}
)

SELECT
    transit_zone,
    wkb_geometry,
    ROW_NUMBER() OVER (ORDER BY transit_zone) AS decomposed_id
FROM decomposed
