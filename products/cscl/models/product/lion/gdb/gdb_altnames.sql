{{ config(materialized='table') }}

-- AltNames table for the published LION GDB (nyclion_*.zip). Per ETL spec §2.7.4:
-- one set of records per Join_ID occurring in the LION feature class, each carrying a
-- street/feature name belonging to a B7SC implicit in that Join_ID. The implicit B7SCs
-- are the segment's B5SC (StreetCode) concatenated with each of its LGCs. Names come
-- from the CSCL StreetName / FeatureName tables (excluding StreetName DCP_FLAG = 'N').
--
-- Scope: non-SAF Join_IDs only. SAF-replicant Join_IDs (…X/…N-suffixed) use a different
-- B7SC encoding (spec §2.7.3) and are not yet produced — see _gdb.yml.
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

SELECT DISTINCT
    names.pdir AS "PDir",
    names.ptype AS "PType",
    names.sname AS "SName",
    names.stype AS "SType",
    names.sdir AS "SDir",
    names.street AS "Street",
    b7scs.join_id AS "Join_ID"
FROM b7scs
INNER JOIN names ON b7scs.b7sc = names.b7sc
