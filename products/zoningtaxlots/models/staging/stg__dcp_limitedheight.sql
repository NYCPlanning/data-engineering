WITH dcp_limitedheight AS (
    SELECT * FROM {{ source('recipe_sources', 'dcp_limitedheight') }}
),

rename AS (
    SELECT 
        ogc_fid,
        wkb_geometry AS geom,
        lhlbl
    FROM dcp_limitedheight
)

SELECT * FROM rename 
