{{ config(materialized='table') }}

-- AltNames table for the published LION GDB (nyclion_*.zip). Per ETL spec §2.7.4:
-- one set of records per Join_ID occurring in the LION feature class, each carrying a
-- street/feature name belonging to a B7SC in that Join_ID. Names come from the CSCL
-- StreetName / FeatureName tables (excluding StreetName DCP_FLAG = 'N').
--
-- For regular segments: B7SCs are implicit (B5SC from StreetCode field + each LGC)
-- For SAF segments: B7SCs are explicit (B5SC and LGCs directly in Join_ID per spec §2.7.3)
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
b7scs AS (
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

-- SAF segments (G, N, X from CommonPlace)
saf_segments_gnx AS (
    SELECT DISTINCT
        {{ saf_join_id() }} AS join_id,
        b5sc,
        lgc1,
        lgc2,
        lgc3,
        lgc4
    FROM {{ ref('int__saf_gnx') }}
),

-- SAF segments (S from AddressPoints)
saf_segments_s AS (
    SELECT DISTINCT
        {{ saf_join_id() }} AS join_id,
        b5sc,
        lgc1,
        lgc2,
        lgc3,
        lgc4
    FROM {{ ref('int__saf_s') }}
),

-- SAF segments (V from AddressPoints)
saf_segments_v AS (
    SELECT DISTINCT
        {{ saf_join_id() }} AS join_id,
        b5sc,
        lgc1,
        lgc2,
        lgc3,
        lgc4
    FROM {{ ref('int__saf_v') }}
),

-- All SAF segments combined
saf_segments AS (
    SELECT * FROM saf_segments_gnx
    UNION ALL
    SELECT * FROM saf_segments_s
    UNION ALL
    SELECT * FROM saf_segments_v
),

-- B7SCs explicit in SAF Join_IDs: b5sc (from Join_ID) + each non-null LGC
saf_b7scs AS (
    SELECT DISTINCT
        saf_segments.join_id,
        lpad(saf_segments.b5sc, 6, '0') || lgc.lgc AS b7sc
    FROM saf_segments
    CROSS JOIN
        LATERAL (
            VALUES (saf_segments.lgc1), (saf_segments.lgc2), (saf_segments.lgc3), (saf_segments.lgc4)
        ) AS lgc (lgc)
    WHERE lgc.lgc IS NOT null
),

-- names keyed by B7SC. StreetName carries ESRI's parsed components; FeatureName puts
-- the whole name in SName (spec §2.7.4).
names AS (
    SELECT
        b7sc,
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
        null AS pdir,
        null AS ptype,
        feature_name AS sname,
        null AS stype,
        null AS sdir,
        lookup_key AS street
    FROM {{ source('recipe_sources', 'dcp_cscl_featurename') }}
)

-- Regular (non-SAF) altnames
SELECT DISTINCT
    names.pdir AS "PDir",
    names.ptype AS "PType",
    names.sname AS "SName",
    names.stype AS "SType",
    names.sdir AS "SDir",
    names.street AS "Street",
    b7scs.join_id AS "Join_ID",
    -- Composite key for QA comparison (street + join_id)
    names.street || '|' || b7scs.join_id AS _altnames_key
FROM b7scs
INNER JOIN names ON b7scs.b7sc = names.b7sc

UNION ALL

-- SAF altnames
SELECT DISTINCT
    names.pdir AS "PDir",
    names.ptype AS "PType",
    names.sname AS "SName",
    names.stype AS "SType",
    names.sdir AS "SDir",
    names.street AS "Street",
    saf_b7scs.join_id AS "Join_ID",
    names.street || '|' || saf_b7scs.join_id AS _altnames_key
FROM saf_b7scs
INNER JOIN names ON saf_b7scs.b7sc = names.b7sc
