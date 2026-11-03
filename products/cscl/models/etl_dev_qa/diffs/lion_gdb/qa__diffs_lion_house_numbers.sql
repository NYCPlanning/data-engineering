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
-- all-blank/all-zero outcome. Prod never applies the rule: the tool's main flow never calls
-- ImportLION2_ToGDB, where it lives (BytesLION_CSCL_workflow_tool_PUB.py:326, the call at
-- :621 commented out), and the ETL copies the source's house numbers
-- (ExtractorClass.cs:2993-3072). Neither do we by default, so G rows are checked only with
-- zero_generic_house_numbers on (CSCL-LION-15), F rows only with
-- zero_f_segment_house_numbers on (CSCL-LION-24), and rows of FeatureTyp other than 0, 6
-- or W only with zero_feature_type_house_numbers on (CSCL-LION-29). With all three off
-- (the default) this is empty.
--
-- "changes" itself DOES follow the usual convention (old=prod, new=dev): for each
-- violating field, old is prod's real (buggy) value, new is our dev build's actual
-- value for that same field - plain NULL for FromLeft/FromRight/ToLeft/ToRight,
-- since gdb_lion.sql has never implemented those (tracked separately in
-- compare_gdb.py's KNOWN_NULL_COLUMNS) - that's honest, not a reason to hide them.
-- Only the fields that actually violate the rule appear as keys (null keys are filtered out).

WITH violations AS (
    SELECT *
    FROM {{ ref('qa_int__prod_fgdb_lion') }}
    WHERE
        -- G only with zero_generic_house_numbers on, F only with zero_f_segment_house_numbers
        -- on, FeatureTyp other than 0, 6 or W only with zero_feature_type_house_numbers on:
        -- prod's LION gdb keeps their house numbers, and so do we by default
        -- (CSCL-LION-15, CSCL-LION-24, CSCL-LION-29)
        (
            {% if var('zero_generic_house_numbers') %}segmenttyp = 'G' OR {% endif -%}
            {% if var('zero_f_segment_house_numbers') %}segmenttyp = 'F' OR {% endif -%}
            {% if var('zero_feature_type_house_numbers') %}featuretyp NOT IN ('0', '6', 'W') OR {% endif -%}
            FALSE
        )
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
-- unique for ~0.7% of gdb_lion (SAF-replicant multiplicity); picking one arbitrarily here is fine since this is read-only
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
    to_json(map_from_entries(list_filter([
        { 'k': 'llo_hyphen', 'v': CASE
            WHEN nullif(v.llo_hyphen, '') IS NOT NULL
                THEN json_object('old', v.llo_hyphen, 'new', d.llo_hyphen)
        END::JSON },
        { 'k': 'lhi_hyphen', 'v': CASE
            WHEN nullif(v.lhi_hyphen, '') IS NOT NULL
                THEN json_object('old', v.lhi_hyphen, 'new', d.lhi_hyphen)
        END::JSON },
        { 'k': 'rlo_hyphen', 'v': CASE
            WHEN nullif(v.rlo_hyphen, '') IS NOT NULL
                THEN json_object('old', v.rlo_hyphen, 'new', d.rlo_hyphen)
        END::JSON },
        { 'k': 'rhi_hyphen', 'v': CASE
            WHEN nullif(v.rhi_hyphen, '') IS NOT NULL
                THEN json_object('old', v.rhi_hyphen, 'new', d.rhi_hyphen)
        END::JSON },
        { 'k': 'fromleft', 'v': CASE
            WHEN coalesce(v.fromleft, 0) != 0
                THEN json_object('old', v.fromleft, 'new', d.fromleft)
        END::JSON },
        { 'k': 'fromright', 'v': CASE
            WHEN coalesce(v.fromright, 0) != 0
                THEN json_object('old', v.fromright, 'new', d.fromright)
        END::JSON },
        { 'k': 'toleft', 'v': CASE
            WHEN coalesce(v.toleft, 0) != 0
                THEN json_object('old', v.toleft, 'new', d.toleft)
        END::JSON },
        { 'k': 'toright', 'v': CASE
            WHEN coalesce(v.toright, 0) != 0
                THEN json_object('old', v.toright, 'new', d.toright)
        END::JSON }
    ], e -> e.v IS NOT NULL))) AS changes,
    'lion_house_numbers' AS output_file_id,
    -- G, F and FeatureTyp rows are here only with their var on: we zero them, as the spec
    -- (G) and the legacy tool's rule (G, F, FeatureTyp) do, where prod's LION gdb keeps the
    -- source's house numbers (CSCL-LION-15, CSCL-LION-24, CSCL-LION-29). A G or F row of
    -- such a FeatureTyp is labelled by its SegmentTyp when that var is on.
    CASE
        WHEN
            {{ 'TRUE' if var('zero_generic_house_numbers') else 'FALSE' }} AND v.segmenttyp = 'G'
            THEN 'CSCL-LION-15: house numbers kept on generic segments in prod'
        WHEN
            {{ 'TRUE' if var('zero_f_segment_house_numbers') else 'FALSE' }} AND v.segmenttyp = 'F'
            THEN 'CSCL-LION-24: house numbers kept on SegmentTyp F in prod'
        WHEN
            v.featuretyp NOT IN ('0', '6', 'W')
            THEN 'CSCL-LION-29: house numbers kept in prod on a FeatureTyp other than 0, 6 or W'
        ELSE 'House numbers kept in prod'
    END AS diff_group,
    '' AS subgroup,
    '_lion_gdb_key' AS comparison_column,
    'gdb_lion' AS build_table_name,
    'qa_int__prod_fgdb_lion' AS production_table_name,
    -- the G and FeatureTyp rows are the known differences zero_generic_house_numbers and
    -- zero_feature_type_house_numbers turn on; the F rows are left unaccounted, as before
    coalesce(
        ({{ 'TRUE' if var('zero_generic_house_numbers') else 'FALSE' }} AND v.segmenttyp = 'G')
        OR (
            {{ 'TRUE' if var('zero_feature_type_house_numbers') else 'FALSE' }}
            AND v.featuretyp NOT IN ('0', '6', 'W')
        ),
        FALSE
    ) AS accounted_for
FROM violations AS v
LEFT JOIN dev AS d ON v._lion_gdb_key = d._lion_gdb_key
