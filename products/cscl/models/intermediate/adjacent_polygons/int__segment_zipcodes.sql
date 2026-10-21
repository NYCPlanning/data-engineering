{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['globalid']},
    ]
) }}

WITH segment_offsets AS (
    SELECT * FROM {{ ref("int__segment_offsets") }}
)
SELECT
    so.globalid,
    so.lionkey,
    so.segmentid,
    leftzip.zip_code AS l_zip,
    rightzip.zip_code AS r_zip
FROM segment_offsets AS so
-- Overlapping ZipCode polygons can contain the same offset point. Legacy takes the first
-- intersecting ZipCode feature (ShorelineExtractFile.GetZipCode: ftrList[0]), i.e. lowest
-- OBJECTID; ogc_fid is the equivalent here, so each side resolves to exactly one zip.
LEFT JOIN LATERAL (
    SELECT z.zip_code
    FROM {{ source("recipe_sources", "dcp_cscl_zipcode") }} AS z
    WHERE st_within(so.left_offset_point, z.geom)
    ORDER BY z.ogc_fid
    LIMIT 1
) AS leftzip ON TRUE
LEFT JOIN LATERAL (
    SELECT z.zip_code
    FROM {{ source("recipe_sources", "dcp_cscl_zipcode") }} AS z
    WHERE st_within(so.right_offset_point, z.geom)
    ORDER BY z.ogc_fid
    LIMIT 1
) AS rightzip ON TRUE
