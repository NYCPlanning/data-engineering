{{ config(
    materialized = 'table'
) }}

-- Special address flag (LION L48) of each ramp proto-segment (ALT_SEGDATA_TYPE R).
-- CSCL-LION-19: prod splits a centerline's SAF records between the centerline and its
-- proto-segments (CenterlineExtractFile.cs:2665, GetSpecialAddressFlag). For a ramp proto
-- (:2716-2737) it gives the proto an AltSegmentData SAF row with the proto's LIONKEY and an
-- address point whose B7SC_ACTUAL starts with the proto's B5SC (a CommonPlace stays with the
-- centerline); a borough boundary proto (:2706-2714) gets the records of its own borough.
-- It collects them over all the centerline's proto-segments and writes each proto the
-- first of S, N, G, else the first record (:2748-2762). So a ramp on its centerline's
-- street code carries the address points' flag. The legacy rule (and the fix, var
-- fix_ramp_proto_special_address) says a ramp never carries one.
WITH protos AS (
    SELECT
        globalid,
        segmentid,
        ogc_fid,
        alt_segdata_type,
        lionkey,
        b5sc,
        borough
    FROM {{ ref("stg__altsegmentdata_proto") }}
),
ramps AS (
    SELECT * FROM protos
    WHERE alt_segdata_type = 'R'
),
centerline_borough AS (
    SELECT
        segmentid,
        ANY_VALUE(boroughcode) AS boroughcode
    FROM {{ source("recipe_sources", "dcp_cscl_centerline") }}
    GROUP BY segmentid
),
-- prod reads SAF types F as D (CSCLDataHelpersClass.cs, GetSAFTypeDatum)
saf_records AS (
    SELECT
        segmentid,
        1 AS source_rank,
        ogc_fid,
        CASE WHEN UPPER(TRIM(saftype)) = 'F' THEN 'D' ELSE saftype END AS saftype,
        borough::VARCHAR AS boroughcode,
        lionkey,
        NULL::VARCHAR AS b7sc_actual
    FROM {{ ref("stg__altsegmentdata_saf") }}
    WHERE NULLIF(TRIM(saftype), '') IS NOT NULL
    UNION ALL
    SELECT
        segmentid,
        2 AS source_rank,
        ogc_fid,
        CASE WHEN UPPER(TRIM(special_condition)) = 'F' THEN 'D' ELSE special_condition END AS saftype,
        boroughcode::VARCHAR AS boroughcode,
        NULL::VARCHAR AS lionkey,
        b7sc_actual
    FROM {{ source("recipe_sources", "dcp_cscl_addresspoints") }}
    WHERE NULLIF(TRIM(special_condition), '') IS NOT NULL
),
-- the records prod gives the proto-segments of each centerline
proto_records AS (
    SELECT
        protos.segmentid,
        protos.ogc_fid AS proto_ogc_fid,
        saf_records.source_rank,
        saf_records.ogc_fid,
        saf_records.saftype
    FROM protos
    INNER JOIN saf_records ON protos.segmentid = saf_records.segmentid
    LEFT JOIN centerline_borough ON protos.segmentid = centerline_borough.segmentid
    WHERE
        (
            protos.alt_segdata_type = 'R'
            AND (
                (saf_records.source_rank = 1 AND saf_records.lionkey = protos.lionkey)
                OR (saf_records.source_rank = 2 AND LEFT(saf_records.b7sc_actual, 6) = protos.b5sc)
            )
        )
        OR (
            protos.alt_segdata_type = 'B'
            AND saf_records.boroughcode IS DISTINCT FROM centerline_borough.boroughcode::VARCHAR
            AND saf_records.boroughcode = protos.borough::VARCHAR
        )
),
prod_flag AS (
    SELECT
        segmentid,
        saftype AS special_address_flag
    FROM proto_records
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY segmentid
        ORDER BY
            CASE saftype WHEN 'S' THEN 1 WHEN 'N' THEN 2 WHEN 'G' THEN 3 ELSE 4 END,
            proto_ogc_fid, source_rank, ogc_fid
    ) = 1
)
SELECT
    ramps.globalid,
    ramps.segmentid,
    prod_flag.special_address_flag AS special_address_flag_prod,
    NULL::VARCHAR AS special_address_flag_fixed,
    {% if var('fix_ramp_proto_special_address') -%}
        special_address_flag_fixed
    {%- else -%}
        special_address_flag_prod
    {%- endif %} AS special_address_flag
FROM ramps
LEFT JOIN prod_flag ON ramps.segmentid = prod_flag.segmentid
