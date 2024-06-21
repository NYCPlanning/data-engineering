WITH dcp_specialpurpose AS (
    SELECT * FROM {{ source('recipe_sources', 'dcp_specialpurpose') }}
),

rename AS (
    SELECT 
        ogc_fid,
        sdlbl,
        wkb_geometry AS geom
    FROM dcp_specialpurpose
)

SELECT * FROM rename

