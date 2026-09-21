{{ config(materialized='table') }}

-- SAF-replicant Join_IDs for gdb_altnames, for the two SAF source types whose B7SC has no
-- relationship to their associated segment's own street classification: CommonPlace (a
-- park/plaza/etc. can point at a segment while carrying a totally unrelated B7SC) and
-- AddressPoint (vanity/alternate addressing, B7SC_VANITY/B7SC_ACTUAL). Unlike
-- ALTSEGMENTDATA-sourced SAF records - whose B5SC/LGCs already populate the segment's own
-- output LGC1-4 fields, so the regular (non-SAF) Join_ID/AltNames path already covers them -
-- these two have no such overlap and are pure gaps in gdb_altnames.sql's current scope.
--
-- Formula from the legacy ETL's GetSAFBytesJoinID (ExtractorClass.cs): Borough (from the
-- resolved LION segment, not the SAF row's own borough field) + street code (5 bytes - the
-- B7SC's 6-byte B5SC portion *without* its own leading borough digit, since that's already
-- supplied by the segment) + up to 4 LGC slots (2 bytes each, zero-filled) + SAFType(1) =
-- 15 chars, no trailing spaces (unlike the non-SAF form's trailing "  "). B7SC itself is 8
-- bytes: 6-byte B5SC (borough + 5-digit street code) + 2-byte LGC - confirmed against real
-- data (dcp_cscl_streetname.b7sc, e.g. '11001001' -> b5sc '110010', lgc '01'). commonplace
-- only ever populates one LGC slot (implicit in its own B7SC); addresspoint populates one
-- slot from its B7SC_VANITY/B7SC_ACTUAL, then up to 3 more from ADDRESSPOINTLGCS (excluding
-- BOE_LGC = 'Y', ordered by ogc_fid - the legacy code has no explicit order, this is a
-- reasonable stand-in for whatever cursor order ArcObjects returned).
--
-- implicit_b7scs carries the *full* 8-byte B7SC(s) this Join_ID implies, for the name
-- lookup in gdb_altnames.sql - not to be confused with the Join_ID string's own 5-byte
-- (boro-stripped) street-code segment.
--
-- Reuses int__saf_segments's existing segment resolution (roadbed-pointer-list generic
-- mapping, inclusion via int__lion) so a SAF record only contributes here if it resolves to
-- a segment that's actually in the published LION gdb.

WITH saf_segments AS (
    SELECT * FROM {{ ref('int__saf_segments') }}
    WHERE saf_source_table IN ('commonplace', 'addresspoints')
),

lion AS (
    SELECT
        segmentid,
        boroughcode
    FROM {{ ref('int__lion') }}
    WHERE include_in_bytes_lion
),

commonplace_join_ids AS (
    SELECT
        saf.segmentid,
        saf.saf_globalid,
        lion.boroughcode
        || substring(cp.b7sc, 2, 5)
        || lpad(right(cp.b7sc, 2), 2, '0')
        || '000000'
        || saf.saftype AS join_id,
        ARRAY[cp.b7sc] AS implicit_b7scs
    FROM saf_segments AS saf
    INNER JOIN lion ON saf.segmentid = lion.segmentid
    INNER JOIN {{ source('recipe_sources', 'dcp_cscl_commonplace_gdb') }} AS cp
        ON saf.saf_globalid = cp.globalid
    WHERE saf.saf_source_table = 'commonplace' AND length(cp.b7sc) = 8
),

addresspoint_lgcs AS (
    SELECT
        addresspointid,
        lgc,
        row_number() OVER (PARTITION BY addresspointid ORDER BY ogc_fid) AS rn
    FROM {{ source('recipe_sources', 'dcp_cscl_addresspoint_lgcs') }}
    WHERE boe_lgc IS DISTINCT FROM 'Y'
),

addresspoint_b7sc AS (
    SELECT
        ap.globalid,
        ap.addresspointid,
        CASE ap.special_condition
            WHEN 'V' THEN ap.b7sc_vanity
            WHEN 'S' THEN ap.b7sc_actual
        END AS b7sc
    FROM {{ source('recipe_sources', 'dcp_cscl_addresspoints') }} AS ap
    WHERE ap.special_condition IN ('S', 'V')
),

addresspoint_join_ids AS (
    SELECT
        saf.segmentid,
        saf.saf_globalid,
        lion.boroughcode
        || substring(ab.b7sc, 2, 5)
        || lpad(right(ab.b7sc, 2), 2, '0')
        || lpad(coalesce(l1.lgc, '00'), 2, '0')
        || lpad(coalesce(l2.lgc, '00'), 2, '0')
        || lpad(coalesce(l3.lgc, '00'), 2, '0')
        || saf.saftype AS join_id,
        array_remove(
            ARRAY[
                ab.b7sc,
                CASE WHEN l1.lgc IS NOT NULL THEN left(ab.b7sc, 6) || lpad(l1.lgc, 2, '0') END,
                CASE WHEN l2.lgc IS NOT NULL THEN left(ab.b7sc, 6) || lpad(l2.lgc, 2, '0') END,
                CASE WHEN l3.lgc IS NOT NULL THEN left(ab.b7sc, 6) || lpad(l3.lgc, 2, '0') END
            ],
            NULL
        ) AS implicit_b7scs
    FROM saf_segments AS saf
    INNER JOIN lion ON saf.segmentid = lion.segmentid
    INNER JOIN addresspoint_b7sc AS ab ON saf.saf_globalid = ab.globalid
    LEFT JOIN addresspoint_lgcs AS l1 ON ab.addresspointid = l1.addresspointid AND l1.rn = 1
    LEFT JOIN addresspoint_lgcs AS l2 ON ab.addresspointid = l2.addresspointid AND l2.rn = 2
    LEFT JOIN addresspoint_lgcs AS l3 ON ab.addresspointid = l3.addresspointid AND l3.rn = 3
    WHERE saf.saf_source_table = 'addresspoints' AND length(ab.b7sc) = 8
)

SELECT * FROM commonplace_join_ids
UNION ALL
SELECT * FROM addresspoint_join_ids
