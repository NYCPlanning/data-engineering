{{ config(materialized='table') }}

-- AltNames table for the published LION GDB (nyclion_*.zip). Per ETL spec §2.7.4:
-- one set of records per Join_ID occurring in the LION feature class, each carrying a
-- street/feature name belonging to a B7SC implicit in that Join_ID. The implicit B7SCs
-- are the segment's B5SC (StreetCode) concatenated with each of its LGCs. Names come
-- from the CSCL StreetName / FeatureName tables (excluding StreetName DCP_FLAG = 'N').
--
-- Scope: non-SAF Join_IDs, plus the commonplace/addresspoint SAF-replicant Join_IDs from
-- int__saf_altnames_join_ids.sql (see that model's docstring for why altsegmentdata-sourced
-- SAF Join_IDs are deliberately excluded - traced against the legacy ETL's own C# source,
-- they're expected to already be covered by the regular segment-level path below).
WITH segments AS (
    SELECT DISTINCT
        {{ lion_join_id() }} AS join_id,
        boroughcode || five_digit_street_code AS b5sc,
        lgc1,
        lgc2,
        lgc3,
        lgc4
    FROM {{ ref('int__lion') }}
    WHERE include_in_bytes_lion
),

-- the B7SCs implicit in each Join_ID: b5sc + each non-null LGC
segment_b7scs AS (
    SELECT DISTINCT
        segments.join_id,
        segments.b5sc || lgc.lgc AS b7sc
    FROM segments
    CROSS JOIN
        LATERAL (
            VALUES (segments.lgc1), (segments.lgc2), (segments.lgc3), (segments.lgc4)
        ) AS lgc (lgc)
    WHERE lgc.lgc IS NOT null
),

-- SAF-replicant Join_IDs (commonplace/addresspoint only - see docstring above), unnested
-- from their implicit_b7scs array the same way segment_b7scs unnests LGC1-4.
saf_b7scs AS (
    SELECT DISTINCT
        saf.join_id,
        b7sc
    FROM {{ ref('int__saf_altnames_join_ids') }} AS saf
    CROSS JOIN LATERAL unnest(saf.implicit_b7scs) AS b7sc
),

-- names keyed by B7SC. StreetName carries ESRI's parsed components; FeatureName puts
-- the whole name in SName (spec §2.7.4).
names AS (
    SELECT
        b7sc,
        principal_flag,
        pre_directional AS pdir,
        pre_type AS ptype,
        street_name AS sname,
        post_type AS stype,
        post_directional AS sdir,
        lookup_key AS street
    FROM {{ source('recipe_sources', 'dcp_cscl_streetname') }}
    WHERE dcp_flag IS DISTINCT FROM 'N'
    UNION ALL
    SELECT
        b7sc,
        principal_flag,
        null AS pdir,
        null AS ptype,
        feature_name AS sname,
        null AS stype,
        null AS sdir,
        lookup_key AS street
    FROM {{ source('recipe_sources', 'dcp_cscl_featurename') }}
),

-- Regular per-segment path: every non-excluded name variant for each implicit B7SC (per
-- ETL spec §2.7.4 - AltNames genuinely lists every StreetName/FeatureName row, not just
-- the principal one, for segment-derived Join_IDs).
segment_altnames AS (
    SELECT DISTINCT
        names.pdir,
        names.ptype,
        names.sname,
        names.stype,
        names.sdir,
        names.street,
        segment_b7scs.join_id
    FROM segment_b7scs
    INNER JOIN names ON segment_b7scs.b7sc = names.b7sc
),

-- SAF-replicant path: principal name only. The legacy ETL's SAF-specific name lookup
-- (SetSAFStreetNameAndCode in ExtractorClass.cs) queries a single PRINCIPAL_FLAG = 'Y'
-- row, not every variant - confirmed necessary empirically: some SAF B7SCs (parks,
-- playgrounds) carry dozens of alternate-spelling FeatureName rows (e.g. B7SC 11254001,
-- "Dr. Ronald E. McNair Playground", has 84 FeatureName rows but exactly 1 principal one)
-- that joining the regular way multiplies out per Join_ID far beyond what prod produces.
saf_altnames AS (
    SELECT DISTINCT
        names.pdir,
        names.ptype,
        names.sname,
        names.stype,
        names.sdir,
        names.street,
        saf_b7scs.join_id
    FROM saf_b7scs
    INNER JOIN names ON saf_b7scs.b7sc = names.b7sc AND names.principal_flag = 'Y'
)

SELECT
    pdir AS "PDir",
    ptype AS "PType",
    -- ETL spec §2.7.4: SName is a 30-byte field, truncated on the right if the
    -- concatenated name is longer - not a length limit we enforce upstream.
    left(sname, 30) AS "SName",
    stype AS "SType",
    sdir AS "SDir",
    street AS "Street",
    join_id AS "Join_ID"
FROM segment_altnames
UNION
SELECT
    pdir,
    ptype,
    left(sname, 30) AS sname,
    stype,
    sdir,
    street,
    join_id
FROM saf_altnames
