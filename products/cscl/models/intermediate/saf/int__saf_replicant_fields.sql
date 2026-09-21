{{ config(materialized='table') }}

-- Normalizes every SAF-replicant source (AltSegmentData types A/B/C/D/E/F/O/P,
-- CommonPlace G/N/X, AddressPoint S/V) into one shape for gdb_lion.sql to build
-- replicant rows from: the resolved lionkey (int__saf_segments.segment_lionkey - the
-- SPECIFIC proto-segment a SAF entry belongs to, not just its segmentid, which can have
-- several coincident proto-segment rows in int__lion), the row's own SpecAddr,
-- house-number/zip overrides (side-gated per ETL spec §2.7.2),
-- SAFStreetName/SAFStreetCode (the two fields that only exist on SAF replicants, per
-- spec §2.7.3), and the SAF-replicant Join_ID (same 15-char
-- Boro+StreetCode+LGC1-4+SpecAddr formula as int__saf_altnames_join_ids, extended here
-- to AltSegmentData too - not needed there since AltSegmentData-sourced names already
-- surface via the regular Join_ID/AltNames path).
--
-- Known v1 simplifications (see docs/prod_bugs/012-saf-replicant-lion-rows.md):
-- - Continuous-parity SAF entries (SOSINDICATOR used as a parity flag rather than a
--   side indicator, on AltSegmentData types A/B/C/E/P) emit one row instead of the
--   spec'd two - affects a minority of those entries.
-- - AddressPoint type S/V zip-code override isn't implemented (left as the base
--   segment's own zip).
-- - LGC1-4/StreetCode/BOE_LGC (the *non*-SAF fields) are never overridden on replicant
--   rows - per spec, only house numbers/zip, SpecAddr, SAFStreetName/Code and Join_ID
--   are SAF-specific; everything else (including geometry) is identical to the base row.

WITH saf_segments AS (
    SELECT * FROM {{ ref('int__saf_segments') }}
),
altsegdata AS (
    SELECT * FROM {{ ref('stg__altsegmentdata_saf') }}
),
street_names AS (
    SELECT
        b7sc,
        lookup_key
    FROM {{ source('recipe_sources', 'dcp_cscl_streetname') }}
    WHERE principal_flag = 'Y'
),
feature_names AS (
    SELECT
        b7sc,
        lookup_key
    FROM {{ source('recipe_sources', 'dcp_cscl_featurename') }}
    WHERE principal_flag = 'Y'
),
address_points AS (
    SELECT * FROM {{ ref('int__address_points') }}
),
altnames_join_ids AS (
    SELECT * FROM {{ ref('int__saf_altnames_join_ids') }}
),
lion AS (
    SELECT
        lionkey,
        boroughcode
    FROM {{ ref('int__lion') }}
    WHERE include_in_bytes_lion
),

-- AltSegmentData: types A, B, C, D, E, F, O, P. SAF Streetname/Streetcode are the
-- principal name of (B5SC + LGC1) - for type A this is normally the same street as the
-- underlying segment (an alternate address range on it), for the others it can differ.
altsegmentdata_fields AS (
    SELECT
        saf.segment_lionkey AS lionkey,
        saf.saf_globalid,
        saf.saf_source_table,
        CASE WHEN altsegdata.saftype = 'F' THEN 'D' ELSE altsegdata.saftype END AS specaddr,
        CASE
            WHEN altsegdata.saftype = 'C' THEN '75 STREET'
            ELSE coalesce(feature_names.lookup_key, street_names.lookup_key)
        END AS saf_street_name,
        altsegdata.b5sc AS saf_street_code,
        -- A/B/C/E/P: house numbers apply directly to both sides (SOSINDICATOR here is a
        -- continuous-parity flag when set, not a side indicator - not replicated, see
        -- module docstring - so both sides just pass through as given).
        -- D/F/O: SOSINDICATOR is a side indicator - only the indicated side is populated.
        -- sqlfluff 4.3.0 can't parse IS DISTINCT FROM inside a CASE WHEN (reproduced in
        -- isolation, no AND needed - a parser bug, not invalid SQL); noqa'd below.
        CASE -- noqa: PRS
            WHEN altsegdata.saftype IN ('D', 'F', 'O') AND altsegdata.sosindicator IS DISTINCT FROM '1' THEN NULL -- noqa: PRS
            ELSE nullif(altsegdata.l_low_hn, '0')
        END AS l_low_hn,
        CASE -- noqa: PRS
            WHEN altsegdata.saftype IN ('D', 'F', 'O') AND altsegdata.sosindicator IS DISTINCT FROM '1' THEN NULL -- noqa: PRS
            ELSE nullif(altsegdata.l_high_hn, '0')
        END AS l_high_hn,
        CASE -- noqa: PRS
            WHEN altsegdata.saftype IN ('D', 'F', 'O') AND altsegdata.sosindicator IS DISTINCT FROM '2' THEN NULL -- noqa: PRS
            ELSE nullif(altsegdata.r_low_hn, '0')
        END AS r_low_hn,
        CASE -- noqa: PRS
            WHEN altsegdata.saftype IN ('D', 'F', 'O') AND altsegdata.sosindicator IS DISTINCT FROM '2' THEN NULL -- noqa: PRS
            ELSE nullif(altsegdata.r_high_hn, '0')
        END AS r_high_hn,
        CASE WHEN altsegdata.saftype IN ('D', 'F') AND altsegdata.sosindicator = '1' THEN altsegdata.zipcode END AS l_zip,
        CASE WHEN altsegdata.saftype IN ('D', 'F') AND altsegdata.sosindicator = '2' THEN altsegdata.zipcode END AS r_zip,
        altsegdata.b5sc AS join_b5sc,
        altsegdata.lgc1 AS join_lgc1,
        altsegdata.lgc2 AS join_lgc2,
        altsegdata.lgc3 AS join_lgc3,
        altsegdata.lgc4 AS join_lgc4,
        NULL::text AS precomputed_join_id,
        saf.roadbed,
        saf.generic
    FROM saf_segments AS saf
    INNER JOIN altsegdata ON saf.saf_globalid = altsegdata.globalid
    LEFT JOIN street_names ON (altsegdata.b5sc || altsegdata.lgc1) = street_names.b7sc
    LEFT JOIN feature_names ON (altsegdata.b5sc || altsegdata.lgc1) = feature_names.b7sc
    -- 'Z' isn't a SAF-replicant type per the ETL spec's AltSegmentData list (A/B/C/D/E/
    -- F/O/P only) - confirmed absent from prod's SpecAddr domain.
    WHERE saf.saf_source_table = 'altsegmentdata' AND altsegdata.saftype <> 'Z'
),

-- CommonPlace: types G, N, X (Non-Addressable Placenames). All house number fields are
-- blank per spec. Join_ID reuses int__saf_altnames_join_ids (already computed for
-- gdb_altnames matching purposes - same formula).
commonplace_fields AS (
    SELECT
        saf.segment_lionkey AS lionkey,
        saf.saf_globalid,
        saf.saf_source_table,
        saf.saftype AS specaddr,
        coalesce(feature_names.lookup_key, street_names.lookup_key) AS saf_street_name,
        left(cp.b7sc, 6) AS saf_street_code,
        NULL::text AS l_low_hn,
        NULL::text AS l_high_hn,
        NULL::text AS r_low_hn,
        NULL::text AS r_high_hn,
        NULL::text AS l_zip,
        NULL::text AS r_zip,
        NULL::text AS join_b5sc,
        NULL::text AS join_lgc1,
        NULL::text AS join_lgc2,
        NULL::text AS join_lgc3,
        NULL::text AS join_lgc4,
        aj.join_id AS precomputed_join_id,
        saf.roadbed,
        saf.generic
    FROM saf_segments AS saf
    INNER JOIN {{ source('recipe_sources', 'dcp_cscl_commonplace_gdb') }} AS cp ON saf.saf_globalid = cp.globalid
    LEFT JOIN street_names ON cp.b7sc = street_names.b7sc
    LEFT JOIN feature_names ON cp.b7sc = feature_names.b7sc
    LEFT JOIN altnames_join_ids AS aj ON saf.saf_globalid = aj.saf_globalid
    WHERE saf.saf_source_table = 'commonplace'
),

-- AddressPoint: types S (actual) and V (vanity). House number/side handling per spec
-- §2.7.3's HYPHEN_TYPE discussion, mirrored from int__saf_v.sql's approach. Zip override
-- (spec §2.7.2) is not implemented in v1 - left as the base segment's own zip.
addresspoint_fields AS (
    SELECT
        saf.segment_lionkey AS lionkey,
        saf.saf_globalid,
        saf.saf_source_table,
        saf.saftype AS specaddr,
        CASE saf.saftype
            WHEN 'V' THEN coalesce(feature_names_vanity.lookup_key, street_names_vanity.lookup_key)
            ELSE coalesce(feature_names_actual.lookup_key, street_names_actual.lookup_key)
        END AS saf_street_name,
        CASE saf.saftype
            WHEN 'V' THEN left(ap.b7sc_vanity, 6)
            ELSE left(ap.b7sc_actual, 6)
        END AS saf_street_code,
        CASE WHEN ap.sosindicator = '1' THEN ap.house_number END AS l_low_hn,
        CASE
            WHEN ap.sosindicator = '1'
                THEN CASE WHEN ap.hyphen_type IN ('R', 'X') THEN ap.house_number_range ELSE ap.house_number END
        END AS l_high_hn,
        CASE WHEN ap.sosindicator = '2' THEN ap.house_number END AS r_low_hn,
        CASE
            WHEN ap.sosindicator = '2'
                THEN CASE WHEN ap.hyphen_type IN ('R', 'X') THEN ap.house_number_range ELSE ap.house_number END
        END AS r_high_hn,
        NULL::text AS l_zip,
        NULL::text AS r_zip,
        NULL::text AS join_b5sc,
        NULL::text AS join_lgc1,
        NULL::text AS join_lgc2,
        NULL::text AS join_lgc3,
        NULL::text AS join_lgc4,
        aj.join_id AS precomputed_join_id,
        saf.roadbed,
        saf.generic
    FROM saf_segments AS saf
    INNER JOIN address_points AS ap ON saf.saf_globalid = ap.globalid
    LEFT JOIN street_names AS street_names_vanity ON ap.b7sc_vanity = street_names_vanity.b7sc
    LEFT JOIN feature_names AS feature_names_vanity ON ap.b7sc_vanity = feature_names_vanity.b7sc
    LEFT JOIN street_names AS street_names_actual ON ap.b7sc_actual = street_names_actual.b7sc
    LEFT JOIN feature_names AS feature_names_actual ON ap.b7sc_actual = feature_names_actual.b7sc
    LEFT JOIN altnames_join_ids AS aj ON saf.saf_globalid = aj.saf_globalid
    WHERE saf.saf_source_table = 'addresspoints'
),

combined AS (
    SELECT * FROM altsegmentdata_fields
    UNION ALL
    SELECT * FROM commonplace_fields
    UNION ALL
    SELECT * FROM addresspoint_fields
),

-- A CommonPlace/AddressPoint SAF entry on a divided (generic+roadbed split) street
-- resolves to two candidate segments via int__saf_segments's roadbed-pointer-list
-- redirect (one per side of the split); a LION replicant needs exactly one row, not
-- both. Confirmed empirically: taking one row per saf_globalid (arbitrarily preferring
-- the roadbed-flagged resolution when both exist) reproduces prod's SpecAddr counts for
-- G/N/X/S/V almost exactly (e.g. N: 5,963 vs prod's 5,963; X: 6,295 vs prod's 6,295).
-- AltSegmentData entries are never split this way (always exactly one row already).
deduped AS (
    SELECT DISTINCT ON (saf_globalid) *
    FROM combined
    ORDER BY saf_globalid ASC, roadbed DESC, generic DESC
)

SELECT
    deduped.lionkey,
    deduped.saf_globalid,
    deduped.saf_source_table,
    deduped.specaddr,
    deduped.saf_street_name,
    deduped.saf_street_code,
    deduped.l_low_hn,
    deduped.l_high_hn,
    deduped.r_low_hn,
    deduped.r_high_hn,
    deduped.l_zip,
    deduped.r_zip,
    coalesce(
        deduped.precomputed_join_id,
        lion.boroughcode
        || substring(deduped.join_b5sc, 2, 5)
        || lpad(coalesce(deduped.join_lgc1, '00'), 2, '0')
        || lpad(coalesce(deduped.join_lgc2, '00'), 2, '0')
        || lpad(coalesce(deduped.join_lgc3, '00'), 2, '0')
        || lpad(coalesce(deduped.join_lgc4, '00'), 2, '0')
        || deduped.specaddr
    ) AS join_id
FROM deduped
INNER JOIN lion ON deduped.lionkey = lion.lionkey
