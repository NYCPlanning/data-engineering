{{
  config(
    materialized='table',
    enabled=var('qa_gdb_lion_diffs'),
    indexes=[{'columns': ['_gdb_lion_key']}]
  )
}}

-- Prod side of the gdb_lion QA diff: every gdb_lion column as lowercase text, plus the
-- pairing key _gdb_lion_key. See qa_int__build_gdb_lion_keyed for the build side.
--
-- Pairing (same rule as the build side): a row's full key (SegmentID|Join_ID|LLo|RLo, trimmed) is used when prod has
-- the same key. Rows whose full key is absent on the build side are re-keyed
-- SegmentID|Join_ID|#n, n = rank among that segment's unmatched rows ordered by house
-- numbers, so they pair up as "modified" instead of an only_in_build/only_in_legacy pair.
-- Exact-duplicate full keys (e.g. blank house numbers) get a |n ordinal so keys are unique.
-- This absorbs prod's inconsistent house-number zeroing (CSCL-LION-15).

{% set prod_relation = adapter.get_relation(
    database = "db-cscl",
    schema = "production_outputs",
    identifier = "fgdb_lion"
) -%}

WITH base AS (
    SELECT
        {{ gdb_lion_text_columns(false) }},
        {{ gdb_lion_fk('segmentid', 'join_id', 'llo_hyphen', 'rlo_hyphen') }} AS _gdb_lion_fk
    FROM {{ prod_relation }}
),
build_fks AS (
    SELECT DISTINCT {{ gdb_lion_fk('"SegmentID"', '"Join_ID"', '"LLo_Hyphen"', '"RLo_Hyphen"') }} AS fk
    FROM {{ ref('gdb_lion') }}
),
ranked AS (
    SELECT
        base.*,
        build_fks.fk IS NULL AS _unmatched,
        row_number() OVER (
            PARTITION BY
                (build_fks.fk IS NULL),
                CASE
                    WHEN build_fks.fk IS NULL THEN concat_ws('|', base.segmentid, rtrim(base.join_id))
                    ELSE base._gdb_lion_fk
                END
            ORDER BY base.llo_hyphen, base.rlo_hyphen, base.lhi_hyphen, base.rhi_hyphen, base.street, base::text
        ) AS _rank
    FROM base
    LEFT JOIN build_fks ON base._gdb_lion_fk = build_fks.fk
)
SELECT
    ranked.*,
    CASE
        WHEN ranked._unmatched THEN concat_ws('|', ranked.segmentid, rtrim(ranked.join_id), '#' || ranked._rank)
        ELSE ranked._gdb_lion_fk || '|' || ranked._rank
    END AS _gdb_lion_key
FROM ranked
