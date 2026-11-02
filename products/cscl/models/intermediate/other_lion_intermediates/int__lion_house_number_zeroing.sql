{{ config(
    materialized = 'table'
) }}

-- Whether the LION gdb zeroes a segment's house numbers: all eight house-number fields, on
-- the segment's own records and its SAF replicants (ETL spec 2.7.3, BL97-104). The legacy
-- tool's rule (BytesLION_CSCL_workflow_tool_PUB.py:353) is SegmentTyp IN ('G', 'F') OR
-- FeatureTyp NOT IN ('0', '6', 'W'). Prod's LION gdb keeps the house numbers of G and F
-- segments: its ETL zeroes G, then overwrites the zeros with the source's values
-- (ExtractorClass.cs:2993-3072, the return at 3070 commented out), so by default both keep
-- them. zero_generic_house_numbers on zeroes G, as the spec and the legacy tool's rule do
-- (CSCL-LION-15); zero_f_segment_house_numbers on zeroes F, as the legacy tool's rule does
-- (CSCL-LION-24). The FeatureTyp part is the same either way.
WITH lion AS (
    SELECT
        globalid,
        segment_type,
        coalesce(feature_type_code, '0') AS feature_type_code,
        coalesce(
            nullif(l_low_hn, '0'), nullif(l_high_hn, '0'), nullif(r_low_hn, '0'), nullif(r_high_hn, '0')
        ) IS NOT NULL AS has_house_numbers
    FROM {{ ref('int__lion') }}
    WHERE include_in_bytes_lion
),
rules AS (
    SELECT
        *,
        feature_type_code NOT IN ('0', '6', 'W') AS zero_for_feature_type,
        segment_type IN ('G', 'F') OR feature_type_code NOT IN ('0', '6', 'W') AS zero_house_numbers_legacy
    FROM lion
)
SELECT
    *,
    zero_for_feature_type
    OR ({{ 'TRUE' if var('zero_generic_house_numbers') else 'FALSE' }} AND segment_type = 'G')
    OR ({{ 'TRUE' if var('zero_f_segment_house_numbers') else 'FALSE' }} AND segment_type = 'F')
        AS zero_house_numbers
FROM rules
