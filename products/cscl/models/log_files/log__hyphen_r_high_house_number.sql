-- CSCL-LOG-OURS-18, every build: how many LION gdb S and V replicants of a HYPHEN_TYPE R
-- address point get no high house number from prod's gdb writer, which reads it from a dash
-- in HOUSE_NUMBER where CSCL stores it in HOUSE_NUMBER_RANGE (CSCL-LION-25, var
-- fix_hyphen_r_high_house_number), and whether the fix is on in this build. One row, also
-- when the count is 0.
WITH changed AS (
    SELECT saf.saf_globalid
    FROM {{ ref('int__saf_replicant_fields') }} AS saf
    INNER JOIN {{ ref('int__lion') }} AS lion
        ON saf.lionkey = lion.lionkey
    INNER JOIN {{ ref('int__lion_house_number_zeroing') }} AS zeroing
        ON lion.globalid = zeroing.globalid
    WHERE
        saf.saf_source_table = 'addresspoints'
        AND saf.is_written
        AND saf.ap_high_hn_prod IS DISTINCT FROM saf.ap_high_hn_fixed
        -- a zeroed segment's replicants get no house numbers either way
        AND NOT zeroing.zero_house_numbers
        -- gdb_lion writes no record of a segment whose Street is blank
        AND trim(coalesce(lion.street, '')) <> ''
)
SELECT
    'warning' AS log_level,
    'hyphen r house number legacy bug' AS error_category,
    'CSCL-LOG-OURS-18' AS log_message_id,
    NULL::VARCHAR AS globalid,
    'addresspoints' AS source_feature_layer,
    NULL::VARCHAR AS record_id_type,
    NULL::VARCHAR AS record_id,
    {% if var('fix_hyphen_r_high_house_number') -%}
    {{ log_message(
        "{} LION gdb S and V replicants of HYPHEN_TYPE R address points have a high house number in "
        "HOUSE_NUMBER_RANGE and none in HOUSE_NUMBER. The R high house number fix is ON in this build "
        "(fix_hyphen_r_high_house_number = true): they get HOUSE_NUMBER_RANGE, where prod's gdb writes none (CSCL-LION-25)",
        ["count(*)"]
    ) }}
    {%- else -%}
    {{ log_message(
        "{} LION gdb S and V replicants of HYPHEN_TYPE R address points have a high house number in "
        "HOUSE_NUMBER_RANGE and none in HOUSE_NUMBER. The R high house number fix is OFF in this build "
        "(fix_hyphen_r_high_house_number = false): they get none, as prod's gdb (CSCL-LION-25)",
        ["count(*)"]
    ) }}
    {%- endif %} AS message,
    count(*) AS record_count,
    NULL::VARCHAR AS output_filename
FROM changed
