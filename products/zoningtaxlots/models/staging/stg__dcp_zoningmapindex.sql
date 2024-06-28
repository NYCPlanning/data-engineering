WITH dcp_zoningmapindex AS (
    SELECT * FROM {{ source('recipe_sources', 'dcp_zoningmapindex') }}
),

rename AS (
    SELECT
        zoning_map,
        wkb_geometry AS geom
    FROM dcp_zoningmapindex
)

SELECT * FROM rename
