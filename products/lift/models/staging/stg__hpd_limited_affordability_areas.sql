WITH laa_raw AS (
    SELECT * FROM {{ source('recipe_sources', 'hpd_limited_affordability_areas') }}
),

final AS (
    SELECT
        ogc_fid AS area_id,
        -- Source shapefile was uploaded to edm-private as a bare .shp, with no
        -- .dbf/.prj sidecars - no attribute columns survive, only geometry (see
        -- README limitations). CRS isn't declared either; relabeled to EPSG:4326
        -- based on coordinate inspection (lon/lat, NYC's range) - same approach
        -- as stg__pluto, not a real ST_Transform.
        ST_SETCRS(geom, 'EPSG:4326') AS geom
    FROM laa_raw
)

SELECT * FROM final
