{{ config(
    materialized = 'table'
) }}
-- Special address flag (LION L48) per segment and borough; int__lion joins it on both.
-- A SAF record counts for the LION record of its own borough, except on a centerline with
-- no borough boundary (ALT_SEGDATA_TYPE B) proto-segment: there legacy gives the centerline
-- every SAF record on it, whatever its borough (CenterlineExtractFile.cs:2665,
-- GetSpecialAddressFlag: with no proto-segments, or with ramp/other ones, the borough is
-- never compared; only a B proto-segment splits the records by borough). So it is keyed on
-- the centerline's borough there (26D: Brooklyn CommonPlaces on Queens segments 0052212
-- and 0052215 flag them X and G in QueensLION.dat, as prod).
WITH centerline_borough AS (
    SELECT
        segmentid,
        any_value(boroughcode) AS boroughcode
    FROM {{ source("recipe_sources", "dcp_cscl_centerline") }}
    GROUP BY segmentid
),
boundary_proto_segments AS (
    SELECT DISTINCT segmentid
    FROM {{ ref("stg__altsegmentdata_proto") }}
    WHERE alt_segdata_type = 'B'
),
altsegment_saf_records AS (
    SELECT * FROM {{ ref("stg__altsegmentdata_saf") }}
),
addresspoints AS (
    SELECT * FROM {{ source("recipe_sources", "dcp_cscl_addresspoints") }}
),
commonplace AS (
    SELECT * FROM {{ source("recipe_sources", "dcp_cscl_commonplace_gdb") }}
),
unioned AS (
    SELECT
        segmentid,
        saftype,
        borough AS boroughcode,
        1 AS source_priority,
        ogc_fid
    FROM altsegment_saf_records
    UNION ALL
    SELECT
        segmentid,
        special_condition AS saftype,
        boroughcode,
        2 AS source_priority,
        ogc_fid
    FROM addresspoints
    UNION ALL
    SELECT
        segmentid,
        saftype,
        boroughcode,
        3 AS source_priority,
        ogc_fid
    FROM commonplace
),
keyed AS (
    SELECT
        unioned.segmentid,
        CASE
            WHEN centerline_borough.segmentid IS NOT NULL AND boundary_proto_segments.segmentid IS NULL
                THEN centerline_borough.boroughcode
            ELSE unioned.boroughcode
        END AS boroughcode,
        unioned.saftype,
        unioned.source_priority,
        unioned.ogc_fid
    FROM unioned
    LEFT JOIN centerline_borough ON unioned.segmentid = centerline_borough.segmentid
    LEFT JOIN boundary_proto_segments ON unioned.segmentid = boundary_proto_segments.segmentid
),
prioritized AS (
    SELECT
        keyed.segmentid,
        keyed.boroughcode,
        keyed.saftype,
        row_number() OVER (
            PARTITION BY segmentid, boroughcode
            -- very specific fields have priority, as specified in seed table
            -- beyond that, order by source and row number
            ORDER BY p.priority, source_priority, ogc_fid
        ) AS row_number
    FROM keyed
    INNER JOIN {{ ref('seed_saf_priority') }} AS p ON keyed.saftype = p.saftype
)
SELECT
    segmentid,
    boroughcode,
    CASE
        -- The SAFTYPE "F" in CSCL is a "D" in LION/Geosupport
        -- Future exports may or may not expect this mapping
        WHEN saftype = 'F' THEN 'D'
        ELSE saftype
    END AS special_address_flag
FROM prioritized
WHERE row_number = 1
