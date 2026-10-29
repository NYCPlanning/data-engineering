-- CSCL-LOG-OURS-09, every build: how many LION proto-segment records of a shoreline or
-- non-street feature, off the borough boundary, prod leaves without a zip where the fix
-- gives them one (CSCL-LION-02, var fix_nonstreet_proto_zip), and whether the fix is on in
-- this build. One row, also when the count is 0.
WITH changed AS (
    SELECT
        lion.lionkey,
        lion.include_in_geosupport_lion
    FROM {{ ref('int__lion') }} AS lion
    INNER JOIN {{ ref('int__segment_zipcodes') }} AS zips
        ON lion.globalid = zips.globalid
    WHERE
        (lion.include_in_geosupport_lion OR lion.include_in_bytes_lion)
        AND lion.source_table = 'altsegmentdata'
        -- a centerline parent's zip comes from the centerline, under either setting
        AND lion.source_class_rank <> 1
        -- a boundary proto takes its own ZIPCODE on one side and none on the other
        AND lion.borough_boundary_indicator IS DISTINCT FROM 'L'
        AND lion.borough_boundary_indicator IS DISTINCT FROM 'R'
        AND (
            zips.l_zip_prod IS DISTINCT FROM zips.l_zip_fixed
            OR zips.r_zip_prod IS DISTINCT FROM zips.r_zip_fixed
        )
)
SELECT
    'warning' AS log_level,
    'proto zip legacy bug' AS error_category,
    'CSCL-LOG-OURS-09' AS log_message_id,
    NULL::VARCHAR AS globalid,
    'altsegmentdata' AS source_feature_layer,
    NULL::VARCHAR AS record_id_type,
    NULL::VARCHAR AS record_id,
    {% if var('fix_nonstreet_proto_zip') -%}
    {{ log_message(
        "{} LION proto-segment records ({} in the LION .dat files) of a shoreline or non-street "
        "feature, off the borough boundary, have a zip on either side. The non-street proto zip fix is ON in this build "
        "(fix_nonstreet_proto_zip = true): they get it, where prod leaves both zips blank (CSCL-LION-02)",
        ["COUNT(*)", "COUNT(*) FILTER (WHERE include_in_geosupport_lion)"]
    ) }}
    {%- else -%}
    {{ log_message(
        "{} LION proto-segment records ({} in the LION .dat files) of a shoreline or non-street "
        "feature, off the borough boundary, have a zip on either side. The non-street proto zip fix is OFF in this build "
        "(fix_nonstreet_proto_zip = false): both zips stay blank, as prod (CSCL-LION-02)",
        ["COUNT(*)", "COUNT(*) FILTER (WHERE include_in_geosupport_lion)"]
    ) }}
    {%- endif %} AS message,
    COUNT(*) AS record_count,
    NULL::VARCHAR AS output_filename
FROM changed
