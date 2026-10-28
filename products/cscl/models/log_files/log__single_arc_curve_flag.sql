-- CSCL-LOG-OURS-07, every build: how many LION records are one circular arc end to end
-- and not a centerline, so prod's curve flag (blank: the source shape has two points) and
-- the fixed one (I) differ (CSCL-LION-03, var fix_single_arc_curve_flag), and whether the
-- fix is on in this build. One row, also when the count is 0.
WITH changed AS (
    SELECT
        lion.lionkey,
        lion.include_in_geosupport_lion
    FROM {{ ref('int__lion') }} AS lion
    INNER JOIN {{ ref('int__noncenterline_curve_flag') }} AS curve
        ON lion.globalid = curve.globalid
    WHERE
        (lion.include_in_geosupport_lion OR lion.include_in_bytes_lion)
        AND curve.curve_flag_prod IS DISTINCT FROM curve.curve_flag_fixed
)
SELECT
    'warning' AS log_level,
    'curve flag legacy bug' AS error_category,
    'CSCL-LOG-OURS-07' AS log_message_id,
    NULL::VARCHAR AS globalid,
    NULL::VARCHAR AS source_feature_layer,
    NULL::VARCHAR AS record_id_type,
    NULL::VARCHAR AS record_id,
    {% if var('fix_single_arc_curve_flag') -%}
    {{ log_message(
        "{} LION records ({} in the LION .dat files; shoreline, subway, rail or non-street) are one "
        "circular arc end to end. "
        "The single-arc curve flag fix is ON in this build (fix_single_arc_curve_flag = true): "
        "they get curve flag I, where prod leaves it blank (CSCL-LION-03)",
        ["COUNT(*)", "COUNT(*) FILTER (WHERE include_in_geosupport_lion)"]
    ) }}
    {%- else -%}
    {{ log_message(
        "{} LION records ({} in the LION .dat files; shoreline, subway, rail or non-street) are one "
        "circular arc end to end. "
        "The single-arc curve flag fix is OFF in this build (fix_single_arc_curve_flag = false): "
        "their curve flag stays blank, as prod, not I (CSCL-LION-03)",
        ["COUNT(*)", "COUNT(*) FILTER (WHERE include_in_geosupport_lion)"]
    ) }}
    {%- endif %} AS message,
    COUNT(*) AS record_count,
    NULL::VARCHAR AS output_filename
FROM changed
