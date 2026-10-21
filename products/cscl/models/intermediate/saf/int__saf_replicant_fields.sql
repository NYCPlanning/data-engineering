{{ config(materialized='table') }}

-- Normalizes every SAF-replicant source (AltSegmentData types A/B/C/D/E/F/O/P,
-- CommonPlace G/N/X, AddressPoint S/V) into one shape for gdb_lion.sql to build
-- replicant rows from: the resolved lionkey (int__saf_segments.segment_lionkey - the
-- SPECIFIC proto-segment a SAF entry belongs to, not just its segmentid, which can have
-- several coincident proto-segment rows in int__lion), the row's own SpecAddr,
-- house-number/zip overrides (side-gated per ETL spec §2.7.2),
-- SAFStreetName/SAFStreetCode (the two fields that only exist on SAF replicants, per
-- spec §2.7.3), and the SAF-replicant Join_ID (same 15-char
-- Boro+StreetCode+LGC1-4+SpecAddr formula as int__saf_altnames_join_ids, which supplies
-- the CommonPlace/AddressPoint ones; computed here for AltSegmentData). gdb_altnames
-- reads its SAF Join_IDs from this model too, so AltNames and LION agree.
--
-- Known gaps (see Bug 012 in data_issues.md):
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
    -- LION carries the plain house number for E/F (Edgewater Park style) addresses; the SAF
    -- section-letter encoding in int__address_points.house_number applies to SAF only.
    SELECT
        * REPLACE (
            CASE
                WHEN snd_feature_type IN ('E', 'F') AND hyphen_type IS DISTINCT FROM 'R'
                    THEN plain_house_number
                ELSE house_number
            END AS house_number
        )
    FROM {{ ref('int__address_points') }}
),
altnames_join_ids AS (
    SELECT * FROM {{ ref('int__saf_altnames_join_ids') }}
),
lion AS (
    SELECT
        lionkey,
        boroughcode,
        lgc1
    FROM {{ ref('int__lion') }}
    WHERE include_in_bytes_lion
),

-- AltSegmentData: types A, B, C, D, E, F, O, P. SAF Streetname/Streetcode are the
-- principal name of (B5SC + LGC1) - type A uses the underlying segment's LGC1 (below), the
-- others the entry's own.
altsegmentdata_fields AS (
    SELECT
        saf.segment_lionkey AS lionkey,
        saf.saf_globalid,
        saf.saf_source_table,
        CASE WHEN altsegdata.saftype = 'F' THEN 'D' ELSE altsegdata.saftype END AS specaddr,
        coalesce(feature_names.lookup_key, street_names.lookup_key) AS saf_street_name,
        altsegdata.b5sc AS saf_street_code,
        -- A/B/C/E/P: both ranges pass through as given; when SOSINDICATOR is set it is a
        -- continuous-parity flag (parity_side below) and gdb_lion splits them onto that side.
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
        saf.generic,
        -- A/B/C/E/P: SOSINDICATOR is a continuous-parity flag (1 = L, 2 = R) when set -
        -- gdb_lion writes these entries as two rows (ETL spec §2.7.1/§2.7.2).
        CASE
            WHEN altsegdata.saftype IN ('A', 'B', 'C', 'E', 'P') AND altsegdata.sosindicator = '1' THEN 'L'
            WHEN altsegdata.saftype IN ('A', 'B', 'C', 'E', 'P') AND altsegdata.sosindicator = '2' THEN 'R'
        END AS parity_side
    FROM saf_segments AS saf
    INNER JOIN altsegdata ON saf.saf_globalid = altsegdata.globalid
    INNER JOIN lion AS seg ON saf.segment_lionkey = seg.lionkey
    -- Type A names its street with the underlying segment's LGC1 (prod: Boardwalk W entry
    -- on a LGC 03 segment -> BOARDWALK WEST); the other types use the entry's own LGC1.
    LEFT JOIN street_names
        ON (altsegdata.b5sc || CASE WHEN altsegdata.saftype = 'A' THEN seg.lgc1 ELSE altsegdata.lgc1 END) = street_names.b7sc
    LEFT JOIN feature_names
        ON (altsegdata.b5sc || CASE WHEN altsegdata.saftype = 'A' THEN seg.lgc1 ELSE altsegdata.lgc1 END) = feature_names.b7sc
    -- 'Z' isn't a SAF-replicant type per the ETL spec's AltSegmentData list (A/B/C/D/E/
    -- F/O/P only) - confirmed absent from prod's SpecAddr domain.
    WHERE saf.saf_source_table = 'altsegmentdata' AND altsegdata.saftype <> 'Z'
),

-- CommonPlace: types G, N, X (Non-Addressable Placenames). All house number fields are
-- blank per spec. Join_ID from int__saf_altnames_join_ids.
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
        saf.generic,
        NULL::text AS parity_side
    FROM saf_segments AS saf
    INNER JOIN {{ source('recipe_sources', 'dcp_cscl_commonplace_gdb') }} AS cp ON saf.saf_globalid = cp.globalid
    LEFT JOIN street_names ON cp.b7sc = street_names.b7sc
    LEFT JOIN feature_names ON cp.b7sc = feature_names.b7sc
    LEFT JOIN altnames_join_ids AS aj
        ON saf.saf_globalid = aj.saf_globalid AND saf.segment_lionkey = aj.lionkey
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
        -- House number '0' means none, as in the AltSegmentData branch above.
        CASE WHEN ap.sosindicator = '1' THEN nullif(ap.house_number, '0') END AS l_low_hn,
        CASE
            WHEN ap.sosindicator = '1'
                THEN nullif(
                    CASE WHEN ap.hyphen_type IN ('R', 'X') THEN ap.house_number_range ELSE ap.house_number END, '0'
                )
        END AS l_high_hn,
        CASE WHEN ap.sosindicator = '2' THEN nullif(ap.house_number, '0') END AS r_low_hn,
        CASE
            WHEN ap.sosindicator = '2'
                THEN nullif(
                    CASE WHEN ap.hyphen_type IN ('R', 'X') THEN ap.house_number_range ELSE ap.house_number END, '0'
                )
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
        saf.generic,
        NULL::text AS parity_side
    FROM saf_segments AS saf
    INNER JOIN address_points AS ap ON saf.saf_globalid = ap.globalid
    LEFT JOIN street_names AS street_names_vanity ON ap.b7sc_vanity = street_names_vanity.b7sc
    LEFT JOIN feature_names AS feature_names_vanity ON ap.b7sc_vanity = feature_names_vanity.b7sc
    LEFT JOIN street_names AS street_names_actual ON ap.b7sc_actual = street_names_actual.b7sc
    LEFT JOIN feature_names AS feature_names_actual ON ap.b7sc_actual = feature_names_actual.b7sc
    LEFT JOIN altnames_join_ids AS aj
        ON saf.saf_globalid = aj.saf_globalid AND saf.segment_lionkey = aj.lionkey
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
    deduped.parity_side,
    coalesce(
        deduped.precomputed_join_id,
        lion.boroughcode
        || substring(deduped.join_b5sc, 2, 5)
        || lpad(coalesce(deduped.join_lgc1, '00'), 2, '0')
        || lpad(coalesce(deduped.join_lgc2, '00'), 2, '0')
        || lpad(coalesce(deduped.join_lgc3, '00'), 2, '0')
        || lpad(coalesce(deduped.join_lgc4, '00'), 2, '0')
        || deduped.specaddr
    ) AS join_id,
    -- Legacy writes no record for a CommonPlace with no name, but still counts it in
    -- SegCount (as for the GNX records in int__saf_gnx)
    deduped.saf_source_table <> 'commonplace' OR deduped.saf_street_name IS NOT NULL AS is_written
FROM deduped
INNER JOIN lion ON deduped.lionkey = lion.lionkey
