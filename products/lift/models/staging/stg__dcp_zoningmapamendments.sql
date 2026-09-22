-- Zoning map amendment areas, one polygon per (ulurp_no, sub-area) - a single ULURP
-- application can produce several disjoint mapped areas, so ulurpno is not unique here.
-- CRS is already declared as EPSG:4326 on the source geometry, unlike pluto/laa, so no
-- relabeling is needed.
WITH zoningmapamendments_raw AS (
    SELECT * FROM {{ source('recipe_sources', 'dcp_zoningmapamendments') }}
),

final AS (
    SELECT
        ogc_fid,
        ulurpno,
        status,
        project_na,
        effective,
        wkb_geometry AS geom
    FROM zoningmapamendments_raw
)

SELECT * FROM final
