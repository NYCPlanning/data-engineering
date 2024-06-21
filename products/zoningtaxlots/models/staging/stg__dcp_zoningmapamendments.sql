WITH dcp_zoningmapamendments AS (
    SELECT * FROM {{ source('recipe_sources', 'dcp_zoningmapamendments') }}
),

rename AS (
    SELECT
        ogc_fid,
        wkb_geometry AS geom,
        project_na,
        effective
    FROM dcp_zoningmapamendments
)

SELECT * FROM rename
