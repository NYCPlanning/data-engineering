-- CSCL-LOG-OURS-13, every build: how many LION records of a ramp proto-segment
-- (ALT_SEGDATA_TYPE R) prod gives a special address flag, from the SAF records it
-- attributes to the centerline's proto-segments, where the legacy rule leaves a ramp
-- blank (CSCL-LION-19, var fix_ramp_proto_special_address), and whether the fix is on in
-- this build. One row, also when the count is 0.
WITH changed AS (
    SELECT lion.lionkey
    FROM {{ ref('int__lion') }} AS lion
    INNER JOIN {{ ref('int__ramp_proto_special_address') }} AS ramps
        ON lion.globalid = ramps.globalid
    WHERE
        lion.include_in_geosupport_lion
        AND ramps.special_address_flag_prod IS DISTINCT FROM ramps.special_address_flag_fixed
)
SELECT
    'warning' AS log_level,
    'ramp special address legacy bug' AS error_category,
    'CSCL-LOG-OURS-13' AS log_message_id,
    NULL::VARCHAR AS globalid,
    'altsegmentdata' AS source_feature_layer,
    NULL::VARCHAR AS record_id_type,
    NULL::VARCHAR AS record_id,
    {% if var('fix_ramp_proto_special_address') -%}
    {{ log_message(
        "{} LION .dat records of a ramp proto-segment get a special address flag from their "
        "centerline's SAF records in prod. The ramp special address fix is ON in this build "
        "(fix_ramp_proto_special_address = true): their flag is blank, as the legacy rule says (CSCL-LION-19)",
        ["COUNT(*)"]
    ) }}
    {%- else -%}
    {{ log_message(
        "{} LION .dat records of a ramp proto-segment get a special address flag from their "
        "centerline's SAF records in prod. The ramp special address fix is OFF in this build "
        "(fix_ramp_proto_special_address = false): they carry it, as prod, where the legacy rule "
        "leaves a ramp blank (CSCL-LION-19)",
        ["COUNT(*)"]
    ) }}
    {%- endif %} AS message,
    COUNT(*) AS record_count,
    NULL::VARCHAR AS output_filename
FROM changed
