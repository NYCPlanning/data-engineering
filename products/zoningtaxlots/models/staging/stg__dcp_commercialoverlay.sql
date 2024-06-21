WITH dcp_commercialoverlay AS (
    SELECT * FROM {{ source('recipe_sources', 'dcp_commercialoverlay') }}
),

rename AS (
    SELECT 
        ogc_fid，
        wkb_geometry AS geom,
        overlay
    FROM dcp_commercialoverlay
)

SELECT * FROM rename
