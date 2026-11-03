-- CSCL-LOG-OURS-23, every build: how many LION .dat records of a centerline (its own and
-- its proto-segments') get a different coincident segment count (L57) from prod's rule
-- (underground subway/rail segments with the centerline's shape, no floor) than from ours
-- (within 1 ft of its midpoint, never below 1), CSCL-LION-14, var fix_coincident_seg_count;
-- how many of them prod's rule writes 0; and which the build writes. One row, also when the
-- count is 0.
WITH changed AS (
    SELECT coincident_seg_count_prod
    FROM {{ ref('int__lion') }}
    WHERE
        include_in_geosupport_lion
        AND coincident_seg_count_prod IS DISTINCT FROM coincident_seg_count_fixed
)
SELECT
    'warning' AS log_level,
    'coincident segment count' AS error_category,
    'CSCL-LOG-OURS-23' AS log_message_id,
    NULL::VARCHAR AS globalid,
    NULL::VARCHAR AS source_feature_layer,
    NULL::VARCHAR AS record_id_type,
    NULL::VARCHAR AS record_id,
    {% if var('fix_coincident_seg_count') -%}
    {{ log_message(
        "{} LION .dat records of centerlines ({} of them 0 by prod's rule) get a different coincident segment count "
        "by prod's rule (underground subway/rail segments with the centerline's shape, no floor) than by ours. "
        "Our coincident segment count is ON in this build (fix_coincident_seg_count = true): they subtract the "
        "underground subway/rail segments within 1 ft of the centerline's midpoint and stay at least 1 (CSCL-LION-14)",
        ["count(*)", "count(*) FILTER (WHERE coincident_seg_count_prod = 0)"]
    ) }}
    {%- else -%}
    {{ log_message(
        "{} LION .dat records of centerlines ({} of them 0 by prod's rule) get a different coincident segment count "
        "by prod's rule (underground subway/rail segments with the centerline's shape, no floor) than by ours. "
        "Our coincident segment count is OFF in this build (fix_coincident_seg_count = false): they get prod's, "
        "where ours subtracts the underground subway/rail segments within 1 ft of the centerline's midpoint and "
        "stays at least 1 (CSCL-LION-14)",
        ["count(*)", "count(*) FILTER (WHERE coincident_seg_count_prod = 0)"]
    ) }}
    {%- endif %} AS message,
    count(*) AS record_count,
    NULL::VARCHAR AS output_filename
FROM changed
