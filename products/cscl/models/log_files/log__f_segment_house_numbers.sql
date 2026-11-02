-- CSCL-LOG-OURS-10, every build: how many LION gdb records of a SegmentTyp F segment (its own
-- records and its SAF replicants) have house numbers, which the legacy tool's rule zeroes
-- and prod's LION gdb keeps (CSCL-LION-24, var zero_f_segment_house_numbers), and whether
-- the build zeroes them. One row, also when the count is 0.
WITH f_segments AS (
    SELECT
        lion.lionkey,
        zeroing.has_house_numbers
    FROM {{ ref('int__lion') }} AS lion
    INNER JOIN {{ ref('int__lion_house_number_zeroing') }} AS zeroing
        ON lion.globalid = zeroing.globalid
    WHERE
        zeroing.zero_house_numbers_g_only IS DISTINCT FROM zeroing.zero_house_numbers_g_and_f
        -- gdb_lion writes no record of a segment whose Street is blank
        AND trim(coalesce(lion.street, '')) <> ''
),
changed AS (
    SELECT 'segment' AS record_type
    FROM f_segments
    WHERE has_house_numbers
    UNION ALL
    SELECT 'saf_replicant' AS record_type
    FROM f_segments
    INNER JOIN {{ ref('int__saf_replicant_fields') }} AS saf
        ON f_segments.lionkey = saf.lionkey AND saf.is_written
    WHERE coalesce(saf.l_low_hn, saf.l_high_hn, saf.r_low_hn, saf.r_high_hn) IS NOT NULL
)
SELECT
    'warning' AS log_level,
    'house number zeroing' AS error_category,
    'CSCL-LOG-OURS-10' AS log_message_id,
    NULL::VARCHAR AS globalid,
    NULL::VARCHAR AS source_feature_layer,
    NULL::VARCHAR AS record_id_type,
    NULL::VARCHAR AS record_id,
    {% if var('zero_f_segment_house_numbers') -%}
    {{ log_message(
        "{} LION gdb records of segment type F ({} SAF replicants) have house numbers. "
        "Zeroing F segments' house numbers is ON in this build (zero_f_segment_house_numbers = true): "
        "they are zeroed, as the legacy tool's rule does, where prod's LION gdb keeps them (CSCL-LION-24)",
        ["count(*)", "count(*) FILTER (WHERE record_type = 'saf_replicant')"]
    ) }}
    {%- else -%}
    {{ log_message(
        "{} LION gdb records of segment type F ({} SAF replicants) have house numbers. "
        "Zeroing F segments' house numbers is OFF in this build (zero_f_segment_house_numbers = false): "
        "they keep them, as prod's LION gdb does, where the legacy tool's rule zeroes them (CSCL-LION-24)",
        ["count(*)", "count(*) FILTER (WHERE record_type = 'saf_replicant')"]
    ) }}
    {%- endif %} AS message,
    count(*) AS record_count,
    NULL::VARCHAR AS output_filename
FROM changed
