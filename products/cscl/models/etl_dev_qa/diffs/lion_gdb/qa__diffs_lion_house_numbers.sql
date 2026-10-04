{{
  config(
    materialized='table',
    tags=['on_demand', 'diffs', 'diffs_lion_gdb']
  )
}}

-- Which ROWS get flagged isn't a standard dev-vs-prod row diff, despite the shared
-- qa__diffs_* schema (kept for qa__diffs_all/reporting compatibility, and because
-- it's still the right home for this check): a row qualifies when PROD's own
-- delivered fgdb_lion violates its own documented rule, not when dev and prod
-- disagree. Per the real legacy GP tool
-- (lion_dist_tool_files/BytesLION_CSCL_workflow_tool_PUB.py in cscl_etl_archive),
-- any row matching SegmentTyp IN ('G','F') OR FeatureTyp NOT IN ('0','6','W')
-- should have LLo_Hyphen/LHi_Hyphen/RLo_Hyphen/RHi_Hyphen blanked and
-- FromLeft/FromRight/ToLeft/ToRight zeroed - unconditionally, no exceptions in the
-- rule itself. This flags prod rows that match the selection but don't match the
-- all-blank/all-zero outcome. See
-- docs/prod_bugs/018-generic-segment-house-numbers-not-zeroed.md.
--
-- "changes" itself DOES follow the usual convention (old=prod, new=dev): for each
-- violating field, old is prod's real (buggy) value, new is our dev build's actual
-- value for that same field - plain NULL for FromLeft/FromRight/ToLeft/ToRight,
-- since gdb_lion.sql has never implemented those (tracked separately in
-- compare_gdb.py's KNOWN_NULL_COLUMNS) - that's honest, not a reason to hide them.
-- Only the fields that actually violate the rule appear as keys (jsonb_strip_nulls).

WITH violations AS (
    SELECT *
    FROM {{ ref('qa_int__prod_fgdb_lion') }}
    WHERE
        (segmenttyp IN ('G', 'F') OR featuretyp NOT IN ('0', '6', 'W'))
        AND (
            nullif(llo_hyphen, '') IS NOT NULL
            OR nullif(lhi_hyphen, '') IS NOT NULL
            OR nullif(rlo_hyphen, '') IS NOT NULL
            OR nullif(rhi_hyphen, '') IS NOT NULL
            OR coalesce(fromleft, 0) != 0
            OR coalesce(fromright, 0) != 0
            OR coalesce(toleft, 0) != 0
            OR coalesce(toright, 0) != 0
        )
),
-- Dev's own values for the same fields, for context only - not what defines which
-- rows get flagged (see header comment). DISTINCT ON: SegmentID|Join_ID isn't
-- unique for ~0.7% of gdb_lion (SAF-replicant multiplicity - CSCL-LION-10 in
-- data_issues.md); picking one arbitrarily here is fine since this is read-only
-- display context, not row identity.
dev AS (
    SELECT DISTINCT ON (_lion_gdb_key)
        _lion_gdb_key,
        trim("LLo_Hyphen") AS llo_hyphen,
        trim("LHi_Hyphen") AS lhi_hyphen,
        trim("RLo_Hyphen") AS rlo_hyphen,
        trim("RHi_Hyphen") AS rhi_hyphen,
        "FromLeft" AS fromleft,
        "FromRight" AS fromright,
        "ToLeft" AS toleft,
        "ToRight" AS toright
    FROM (
        SELECT
            *,
            concat_ws('|', "SegmentID", rtrim("Join_ID")) AS _lion_gdb_key
        FROM {{ ref('gdb_lion') }}
    ) AS keyed
    ORDER BY _lion_gdb_key
)
SELECT
    v._lion_gdb_key AS comparison_id,
    'modified' AS status,
    jsonb_strip_nulls(jsonb_build_object(
        'llo_hyphen',
        CASE
            WHEN nullif(v.llo_hyphen, '') IS NOT NULL
                THEN jsonb_build_object('old', v.llo_hyphen, 'new', d.llo_hyphen)
        END,
        'lhi_hyphen',
        CASE
            WHEN nullif(v.lhi_hyphen, '') IS NOT NULL
                THEN jsonb_build_object('old', v.lhi_hyphen, 'new', d.lhi_hyphen)
        END,
        'rlo_hyphen',
        CASE
            WHEN nullif(v.rlo_hyphen, '') IS NOT NULL
                THEN jsonb_build_object('old', v.rlo_hyphen, 'new', d.rlo_hyphen)
        END,
        'rhi_hyphen',
        CASE
            WHEN nullif(v.rhi_hyphen, '') IS NOT NULL
                THEN jsonb_build_object('old', v.rhi_hyphen, 'new', d.rhi_hyphen)
        END,
        'fromleft',
        CASE
            WHEN coalesce(v.fromleft, 0) != 0
                THEN jsonb_build_object('old', v.fromleft, 'new', d.fromleft)
        END,
        'fromright',
        CASE
            WHEN coalesce(v.fromright, 0) != 0
                THEN jsonb_build_object('old', v.fromright, 'new', d.fromright)
        END,
        'toleft',
        CASE
            WHEN coalesce(v.toleft, 0) != 0
                THEN jsonb_build_object('old', v.toleft, 'new', d.toleft)
        END,
        'toright',
        CASE
            WHEN coalesce(v.toright, 0) != 0
                THEN jsonb_build_object('old', v.toright, 'new', d.toright)
        END
    )) AS changes,
    'lion_house_numbers' AS output_file_id,
    -- Bug 018: prod's gp.CalculateField house-number zeroing step for Generic
    -- segments, inconsistently applied. See docs/prod_bugs/018-generic-segment-
    -- house-numbers-not-zeroed.md
    'Bug 018: Generic-segment house numbers not consistently zeroed in prod' AS diff_group,
    '' AS subgroup,
    '_lion_gdb_key' AS comparison_column,
    'gdb_lion' AS build_table_name,
    'qa_int__prod_fgdb_lion' AS production_table_name,
    -- Deliberately FALSE always - the root cause (a manual, inconsistently-applied
    -- prod cleanup step) is a strong hypothesis, not GR-confirmed. Flip once
    -- confirmed - see the bug doc.
    FALSE AS accounted_for
FROM violations AS v
LEFT JOIN dev AS d ON v._lion_gdb_key = d._lion_gdb_key
