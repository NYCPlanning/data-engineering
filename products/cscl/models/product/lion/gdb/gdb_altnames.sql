{{ config(materialized='table') }}

-- AltNames table for the published LION GDB (nyclion_*.zip): the street/feature names for
-- each Join_ID in the LION feature class (ETL spec §2.7.4).
--
-- Follows the legacy ETL's AddAltNames (ExtractorClass.cs), which runs once per LION
-- output record, SAF replicants included, and looks up names for B7SC = L1 (borough) + L5
-- (street code) + each of L6-L9 (LGC1-4), keyed by that record's own Join_ID. On a SAF
-- replicant those fields are mostly the host segment's, not the SAF datum's
-- (CenterlineExtractFile.cs GetValue), so the spec's "B7SCs implicit in the
-- Join_ID" doesn't hold for SAF Join_IDs - see data_issues.md CSCL-LION-09:
--   - L6-L9: always the host segment's LGC1-4.
--   - L5: the AltSegmentData row's street code for types B/C/D/E/F/O/P
--     (GetStreetCodeFromSAF); the host segment's own street code otherwise (A, the
--     CommonPlace types G/N/X, the AddressPoint types S/V).
--   - L1: the SAF row's borough. Always equal to the host segment's borough here (0 of
--     21,973 replicant rows differ), so the host's is used.
-- So a CommonPlace Join_ID such as 30013701000000N (PS 44) lists its host segment's street
-- names, not "PS 44".
--
-- Names: StreetName rows (DCP_FLAG <> 'N'), or FeatureName rows if the B7SC has no
-- StreetName rows (the two tables share no B7SC, so a plain union is equivalent).
-- Legacy dedups on Join_ID + LOOKUP_KEY, first row wins; we take the lowest LGC slot,
-- then the lowest source row.
--
-- Not replicated: prod repeats a FeatureName row longer than 30 characters once per LION
-- record carrying its Join_ID, because legacy truncates the name to 30 before adding it
-- to its dedup list (134 keys, 2,873 extra rows in 26c). The diff key is a hash of
-- the row, so this doesn't show up in qa__diffs_fgdb_altnames.

WITH lion AS (
    SELECT
        lionkey,
        boroughcode,
        five_digit_street_code,
        lgc1,
        lgc2,
        lgc3,
        lgc4,
        {{ lion_join_id() }} AS join_id
    FROM {{ ref('int__lion') }}
    WHERE include_in_bytes_lion
),

-- One row per LION output record: base segments, plus the SAF replicants exactly as
-- gdb_lion.sql builds them (int__saf_replicant_fields on lionkey), so AltNames covers the
-- same SAF Join_IDs LION publishes.
lion_records AS (
    SELECT
        join_id,
        boroughcode || five_digit_street_code AS b5sc,
        lgc1,
        lgc2,
        lgc3,
        lgc4
    FROM lion
    UNION
    SELECT
        saf.join_id,
        lion.boroughcode
        || CASE
            WHEN saf.saf_source_table = 'altsegmentdata' AND saf.specaddr <> 'A'
                THEN right(saf.saf_street_code, 5)
            ELSE lion.five_digit_street_code
        END AS b5sc,
        lion.lgc1,
        lion.lgc2,
        lion.lgc3,
        lion.lgc4
    FROM {{ ref('int__saf_replicant_fields') }} AS saf
    INNER JOIN lion ON saf.lionkey = lion.lionkey
    WHERE saf.is_written
),

record_b7scs AS (
    SELECT DISTINCT
        lion_records.join_id,
        lion_records.b5sc || lgc.lgc AS b7sc,
        lgc.slot
    FROM lion_records
    CROSS JOIN
        LATERAL (
            VALUES
            (1, lion_records.lgc1),
            (2, lion_records.lgc2),
            (3, lion_records.lgc3),
            (4, lion_records.lgc4)
        ) AS lgc (slot, lgc)
    WHERE lgc.lgc IS NOT null
),

-- StreetName carries ESRI's parsed components. SName is the bare street_name unless the
-- row has a pre/post modifier, in which case legacy concatenates the components, each
-- followed by a space, and truncates to 30 (so prod's SName keeps a trailing space, e.g.
-- 'TREMONT AVE BRG '). A pre-modifier-only row omits the post components, a
-- post-modifier-only row the pre ones. FeatureName puts the whole LOOKUP_KEY in SName
-- (not FEATURE_NAME, which can be abbreviated differently: 'DIST BNDY' vs 'DIST BOUNDARY').
-- A null LOOKUP_KEY/STREET_NAME comes out as '' (ArcObjects reads nulls as empty strings).
street_names AS (
    SELECT
        b7sc,
        ogc_fid,
        nullif(pre_directional, '') AS pdir,
        nullif(pre_type, '') AS ptype,
        nullif(post_type, '') AS stype,
        nullif(post_directional, '') AS sdir,
        coalesce(lookup_key, '') AS street,
        nullif(pre_modifier, '') AS pre_modifier,
        nullif(post_modifier, '') AS post_modifier,
        coalesce(street_name, '') AS street_name
    FROM {{ source('recipe_sources', 'dcp_cscl_streetname') }}
    WHERE upper(trim(dcp_flag)) IS DISTINCT FROM 'N'
),

names AS (
    SELECT
        b7sc,
        ogc_fid,
        pdir,
        ptype,
        CASE
            WHEN pre_modifier IS null AND post_modifier IS null THEN street_name
            WHEN post_modifier IS null
                THEN concat_ws(' ', pre_modifier, pdir, ptype, street_name) || ' '
            WHEN pre_modifier IS null
                THEN concat_ws(' ', street_name, sdir, stype, post_modifier) || ' '
            ELSE concat_ws(' ', pre_modifier, pdir, ptype, street_name, sdir, stype, post_modifier) || ' '
        END AS sname,
        stype,
        sdir,
        street
    FROM street_names
    UNION ALL
    SELECT
        b7sc,
        ogc_fid,
        null AS pdir,
        null AS ptype,
        coalesce(lookup_key, '') AS sname,
        null AS stype,
        null AS sdir,
        coalesce(lookup_key, '') AS street
    FROM {{ source('recipe_sources', 'dcp_cscl_featurename') }}
),

altnames AS (
    SELECT DISTINCT ON (record_b7scs.join_id, names.street)
        names.pdir,
        names.ptype,
        names.sname,
        names.stype,
        names.sdir,
        names.street,
        record_b7scs.join_id
    FROM record_b7scs
    INNER JOIN names ON record_b7scs.b7sc = names.b7sc
    ORDER BY record_b7scs.join_id, names.street, record_b7scs.slot, names.ogc_fid
)

SELECT
    pdir AS "PDir",
    ptype AS "PType",
    -- ETL spec §2.7.4: SName is a 30-byte field, truncated on the right.
    left(sname, 30) AS "SName",
    stype AS "SType",
    sdir AS "SDir",
    street AS "Street",
    join_id AS "Join_ID"
FROM altnames
