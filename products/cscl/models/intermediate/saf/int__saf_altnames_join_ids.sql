{{ config(materialized='table') }}

-- SAF-replicant Join_IDs for the CommonPlace (G/N/X) and AddressPoint (S/V) sources, one per
-- resolved (SAF record, host proto-segment). int__saf_replicant_fields computes the
-- AltSegmentData ones itself and takes these for the other two; gdb_lion publishes the
-- result and gdb_altnames reads it from there. (The name is historical: this model no
-- longer feeds gdb_altnames directly - AltNames for a SAF Join_ID come from the host
-- segment's B7SCs, not from this Join_ID's own B7SC; see CSCL-LION-09.)
--
-- Formula from the legacy ETL's GetSAFBytesJoinID (ExtractorClass.cs): borough + street
-- code (5 bytes - the B7SC's 6-byte B5SC without its borough digit) + 4 LGC slots (2
-- bytes each, zero-filled) + SAFType = 15 chars, no trailing spaces (unlike the non-SAF
-- form's trailing "  "). B7SC is 8 bytes: 6-byte B5SC + 2-byte LGC (e.g. '11001001' ->
-- '110010', '01'). CommonPlace fills only the first LGC slot, from its own B7SC;
-- AddressPoint fills it from B7SC_VANITY/B7SC_ACTUAL, then up to 3 more from
-- ADDRESSPOINTLGCS (ordered by ogc_fid - the legacy code has no explicit order).
--
-- Borough: legacy uses the LION record's LBoro (RBoro if blank), which gdb_lion derives
-- from the host proto-segment's boroughcode - so join int__lion on the specific
-- segment_lionkey, not segmentid. A segmentid can have proto-segments in two boroughs;
-- joining on it gave 3 CommonPlace Join_IDs the wrong borough digit (e.g.
-- 10050302000000N vs prod's 20050302000000N, segment 241972). See CSCL-LION-09.
--
-- ADDRESSPOINTLGCS: legacy selects BOE_LGC <> 'Y', which also drops rows where BOE_LGC is
-- null (e.g. address point 5146023: prod 12469002000000V, not 12469002040000V).

WITH saf_segments AS (
    SELECT * FROM {{ ref('int__saf_segments') }}
    WHERE saf_source_table IN ('commonplace', 'addresspoints')
),

lion AS (
    SELECT
        lionkey,
        boroughcode
    FROM {{ ref('int__lion') }}
    WHERE include_in_bytes_lion
),

commonplace_join_ids AS (
    SELECT
        saf.segmentid,
        saf.segment_lionkey AS lionkey,
        saf.saf_globalid,
        lion.boroughcode
        || substring(cp.b7sc, 2, 5)
        || lpad(right(cp.b7sc, 2), 2, '0')
        || '000000'
        || saf.saftype AS join_id
    FROM saf_segments AS saf
    INNER JOIN lion ON saf.segment_lionkey = lion.lionkey
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
    WHERE boe_lgc <> 'Y'
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
        saf.segment_lionkey AS lionkey,
        saf.saf_globalid,
        lion.boroughcode
        || substring(ab.b7sc, 2, 5)
        || lpad(right(ab.b7sc, 2), 2, '0')
        || lpad(coalesce(l1.lgc, '00'), 2, '0')
        || lpad(coalesce(l2.lgc, '00'), 2, '0')
        || lpad(coalesce(l3.lgc, '00'), 2, '0')
        || saf.saftype AS join_id
    FROM saf_segments AS saf
    INNER JOIN lion ON saf.segment_lionkey = lion.lionkey
    INNER JOIN addresspoint_b7sc AS ab ON saf.saf_globalid = ab.globalid
    LEFT JOIN addresspoint_lgcs AS l1 ON ab.addresspointid = l1.addresspointid AND l1.rn = 1
    LEFT JOIN addresspoint_lgcs AS l2 ON ab.addresspointid = l2.addresspointid AND l2.rn = 2
    LEFT JOIN addresspoint_lgcs AS l3 ON ab.addresspointid = l3.addresspointid AND l3.rn = 3
    WHERE saf.saf_source_table = 'addresspoints' AND length(ab.b7sc) = 8
)

SELECT * FROM commonplace_join_ids
UNION ALL
SELECT * FROM addresspoint_join_ids
