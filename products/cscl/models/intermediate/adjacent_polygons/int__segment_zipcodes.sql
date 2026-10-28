{{ config(
    materialized = 'table'
) }}

WITH segment_offsets AS (
    SELECT * FROM {{ ref("int__segment_offsets") }}
),
protos AS (
    SELECT DISTINCT globalid FROM {{ ref("int__segments") }}
    WHERE source_table = 'altsegmentdata'
)
SELECT
    so.globalid,
    so.lionkey,
    so.segmentid,
    protos.globalid IS NOT NULL AS is_proto,
    leftzip.zip_code AS l_zip_fixed,
    rightzip.zip_code AS r_zip_fixed,
    -- CSCL-LION-02: prod gives a proto-segment its parent's stored zip, and a shoreline or
    -- non-street feature parent stores none, so prod writes blank (a centerline parent's
    -- zip comes from the centerline in int__lion, not from here). The var
    -- fix_nonstreet_proto_zip picks the proto's own lookup instead.
    CASE WHEN protos.globalid IS NULL THEN leftzip.zip_code END AS l_zip_prod,
    CASE WHEN protos.globalid IS NULL THEN rightzip.zip_code END AS r_zip_prod,
    {% if var('fix_nonstreet_proto_zip') -%}
        leftzip.zip_code AS l_zip,
        rightzip.zip_code AS r_zip
    {%- else -%}
        l_zip_prod AS l_zip,
        r_zip_prod AS r_zip
    {%- endif %}
FROM segment_offsets AS so
LEFT JOIN protos ON so.globalid = protos.globalid
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
