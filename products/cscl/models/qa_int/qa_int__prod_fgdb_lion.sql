{{
  config(
    materialized='table'
  )
}}

-- QA-only projection of production fgdb_lion, scoped to the fields needed to
-- investigate the Generic-segment house-number zeroing issue (see
-- Bug 018 in data_issues.md).
--
-- _lion_gdb_key (SegmentID|Join_ID) isn't used for a dev/prod join here - this model
-- is consumed standalone by qa__diffs_lion_house_numbers.sql as a prod
-- self-consistency check, not a row pairing - but it's kept as a stable per-row
-- identifier (comparison_id) and for parity with compare_gdb.py's own declared key
-- for this layer (lion_outputs.csv), should a real dev/prod join ever be built
-- against this table later. rtrim(join_id): prod's real fgdb_lion.join_id is
-- missing the spec-mandated trailing padding our own gdb_lion.sql correctly
-- includes (see compare_gdb.py's PADDED_COLUMNS and
-- CSCL-LION-10 in data_issues.md) - a no-op here since prod's copy is already
-- unpadded, kept so this key is built the same way regardless of which side a
-- future join compares against.
--
-- trim(): prod's real llo_hyphen/lhi_hyphen/rlo_hyphen/rhi_hyphen are
-- right-justified, space-padded to their 7-byte field width (e.g. " 64-099") -
-- normalized here so '' (the zeroed-out state the GP script sets) is detected
-- correctly rather than masked by whitespace.

{% set prod_relation = adapter.get_relation(
    database = target.database,
    schema = "production_outputs",
    identifier = "fgdb_lion"
) -%}

SELECT
    segmentid,
    join_id,
    segmenttyp,
    featuretyp,
    trim(llo_hyphen) AS llo_hyphen,
    trim(lhi_hyphen) AS lhi_hyphen,
    trim(rlo_hyphen) AS rlo_hyphen,
    trim(rhi_hyphen) AS rhi_hyphen,
    fromleft,
    fromright,
    toleft,
    toright,
    concat_ws('|', segmentid, rtrim(join_id)) AS _lion_gdb_key
FROM {{ prod_relation }}
