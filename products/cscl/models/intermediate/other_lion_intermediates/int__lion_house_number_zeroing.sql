{{ config(
    materialized = 'table'
) }}

-- Whether the LION gdb zeroes a segment's house numbers: all eight house-number fields, on
-- the segment's own records and its SAF replicants (ETL spec 2.7.3, BL97-104). The legacy
-- tool's rule (BytesLION_CSCL_workflow_tool_PUB.py:353) is SegmentTyp IN ('G', 'F') OR
-- FeatureTyp NOT IN ('0', '6', 'W'). Prod's LION gdb doesn't zero F on 26c, and the spec
-- zeroes generic segments (G) without naming F, so by default F keeps its house numbers;
-- zero_f_segment_house_numbers on zeroes F too (CSCL-LION-24). The FeatureTyp part is the
-- same either way.
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
        segment_type = 'G' OR feature_type_code NOT IN ('0', '6', 'W') AS zero_house_numbers_g_only,
        segment_type IN ('G', 'F') OR feature_type_code NOT IN ('0', '6', 'W') AS zero_house_numbers_g_and_f
    FROM lion
)
SELECT
    *,
    {% if var('zero_f_segment_house_numbers') -%}
        zero_house_numbers_g_and_f
    {%- else -%}
        zero_house_numbers_g_only
    {%- endif %} AS zero_house_numbers
FROM rules
