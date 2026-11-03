{{ config(
    materialized = 'table'
) }}

-- One row per feature of a district layer prod copies from a source feature class
-- unclipped (nybid, nyhez, nycdwi) whose source polygon is invalid: rings that cross, or a
-- shell inside another shell (CSCL-DISTRICTS-04). Prod publishes the polygon as stored
-- (GDBExtractorClass.cs:952-957); ours publishes a valid one (st_makevalid for nybid, the
-- AtomicPolygon topology for nyhez and nycdwi). The default publishes source_geom, as
-- prod; make_valid_district_polygons on publishes the valid one. SHAPE_Area and
-- SHAPE_Length come from the valid polygon either way, as prod's do.
WITH source_polygons AS (
    SELECT
        'nybid' AS source_layer,
        bidid::varchar AS key_value,
        geom
    FROM {{ source('recipe_sources', 'dcp_cscl_businessimprovementdistrict') }}
    UNION ALL
    SELECT
        'nyhez' AS source_layer,
        hurricane_evacuation_zone AS key_value,
        geom
    FROM {{ source('recipe_sources', 'dcp_cscl_hurricaneevacuationzone') }}
    UNION ALL
    SELECT
        'nycdwi' AS source_layer,
        commdist AS key_value,
        geom
    FROM {{ source('recipe_sources', 'dcp_cscl_communitydistrict') }}
)

SELECT
    source_layer,
    key_value,
    geom AS source_geom,
    st_area(geom) AS source_area_sqft,
    st_area(st_makevalid(geom)) AS valid_area_sqft
FROM source_polygons
WHERE geom IS NOT NULL AND NOT st_isvalid(geom)
