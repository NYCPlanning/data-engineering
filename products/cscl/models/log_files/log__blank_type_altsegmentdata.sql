-- CSCL-LOG-OURS-19, every build: how many AltSegmentData rows have a SAF type and a blank
-- ALT_SEGDATA_TYPE, which prod reads as SAF records (SAF and the LION gdb's SAF replicants
-- and SegCount) and spec Table 13 doesn't (CSCL-SAF-03, var fix_blank_type_altsegmentdata),
-- on which segments, and whether the fix is on in this build. One row, also when the count
-- is 0.
WITH changed AS (
    SELECT
        lpad(segmentid::VARCHAR, 7, '0') AS segmentid,
        saftype
    FROM {{ source('recipe_sources', 'dcp_cscl_altsegmentdata') }}
    WHERE
        nullif(trim(alt_segdata_type), '') IS NULL
        AND nullif(trim(saftype), '') IS NOT NULL
)
SELECT
    'warning' AS log_level,
    'blank alt_segdata_type' AS error_category,
    'CSCL-LOG-OURS-19' AS log_message_id,
    NULL::VARCHAR AS globalid,
    'altsegmentdata' AS source_feature_layer,
    NULL::VARCHAR AS record_id_type,
    NULL::VARCHAR AS record_id,
    {% if var('fix_blank_type_altsegmentdata') -%}
    {{ log_message(
        "{} AltSegmentData rows ({}) have a SAF type and a blank ALT_SEGDATA_TYPE. The blank-type fix is ON in "
        "this build (fix_blank_type_altsegmentdata = true): they are left out of SAF and the LION gdb, as spec "
        "Table 13 says, where prod writes them (CSCL-SAF-03)",
        ["count(*)", "coalesce(string_agg(segmentid || ' ' || saftype, ', ' ORDER BY segmentid, saftype), 'none')"]
    ) }}
    {%- else -%}
    {{ log_message(
        "{} AltSegmentData rows ({}) have a SAF type and a blank ALT_SEGDATA_TYPE. The blank-type fix is OFF in "
        "this build (fix_blank_type_altsegmentdata = false): they are SAF records, in SAF and the LION gdb, as "
        "prod writes them, where spec Table 13 leaves them out (CSCL-SAF-03)",
        ["count(*)", "coalesce(string_agg(segmentid || ' ' || saftype, ', ' ORDER BY segmentid, saftype), 'none')"]
    ) }}
    {%- endif %} AS message,
    count(*) AS record_count,
    NULL::VARCHAR AS output_filename
FROM changed
