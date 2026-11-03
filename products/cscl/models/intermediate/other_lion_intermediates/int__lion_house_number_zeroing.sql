{{ config(
    materialized = 'table'
) }}

-- Whether the LION gdb zeroes a segment's house numbers: all eight house-number fields, on
-- the segment's own records and its SAF replicants (ETL spec 2.7.3, BL97-104). The legacy
-- tool's rule (ImportLION2_ToGDB, BytesLION_CSCL_workflow_tool_PUB.py:326-361) is SegmentTyp
-- IN ('G', 'F') OR FeatureTyp NOT IN ('0', '6', 'W'), but the tool's main flow never calls it
-- (the call at :621 is commented out), and prod's ETL zeroes and then overwrites the zeros
-- with the source's values (ExtractorClass.cs SetBytesNormalizedHouseNumbers, from :2988;
-- the return at 3070 commented out), so prod's LION gdb carries the source's house numbers
-- on every row, and by default so do we. Each part of the legacy rule is behind its own var:
-- zero_generic_house_numbers zeroes G, as the spec and the legacy rule do (CSCL-LION-15);
-- zero_f_segment_house_numbers zeroes F (CSCL-LION-24); zero_feature_type_house_numbers
-- zeroes FeatureTyp other than 0, 6 or W (CSCL-LION-29). All three on is the legacy rule.
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
    ({{ 'TRUE' if var('zero_feature_type_house_numbers') else 'FALSE' }} AND zero_for_feature_type)
    OR ({{ 'TRUE' if var('zero_generic_house_numbers') else 'FALSE' }} AND segment_type = 'G')
    OR ({{ 'TRUE' if var('zero_f_segment_house_numbers') else 'FALSE' }} AND segment_type = 'F')
        AS zero_house_numbers
FROM rules
