{{
    config(
        materialized='table',
        indexes=[{'columns': ['geom'], 'type': 'gist'}],
        tags=['pluto_enrichment']
    )
}}

-- Union of all transit zones to identify land coverage
-- Lots outside this union are in water and should have NULL transit zones

SELECT
    ST_UNION(geom) AS geom
FROM {{ ref('stg__dcp_transit_zones') }}
