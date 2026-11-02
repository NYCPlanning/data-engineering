-- CSCL-LOG-OURS-15, every build: how many LION gdb records of a generic segment (SegmentTyp
-- G; its own records and its SAF replicants) have house numbers, which the spec and the
-- legacy tool's rule zero and prod's LION gdb keeps (CSCL-LION-15, var
-- zero_generic_house_numbers), and whether the build zeroes them. A G segment of a FeatureTyp
-- other than 0, 6 or W is zeroed either way and isn't counted. Records are counted as
-- gdb_lion writes them: two for a continuous-parity segment or SAF entry. One row, also when
-- the count is 0.
WITH g_segments AS (
    SELECT lion.*
    FROM {{ ref('int__lion') }} AS lion
    INNER JOIN {{ ref('int__lion_house_number_zeroing') }} AS zeroing
        ON lion.globalid = zeroing.globalid
    WHERE
        zeroing.segment_type = 'G'
        AND NOT zeroing.zero_for_feature_type
        -- gdb_lion writes no record of a segment whose Street is blank
        AND trim(coalesce(lion.street, '')) <> ''
),
records AS (
    SELECT
        'segment' AS record_type,
        hn.*
    FROM g_segments AS lion
    CROSS JOIN
        LATERAL generate_series(
            1, CASE WHEN lion.continuous_parity_flag IN ('L', 'R') THEN 2 ELSE 1 END
        ) AS rng (n)
    CROSS JOIN
        LATERAL (
            {{ lion_house_number_split(
                "nullif(lion.l_low_hn, '0')", "nullif(lion.l_high_hn, '0')",
                "nullif(lion.r_low_hn, '0')", "nullif(lion.r_high_hn, '0')",
                'lion.continuous_parity_flag', 'rng.n', 'TRUE'
            ) }}
        ) AS hn
    UNION ALL
    SELECT
        'saf_replicant' AS record_type,
        hn.*
    FROM g_segments AS lion
    INNER JOIN {{ ref('int__saf_replicant_fields') }} AS saf
        ON lion.lionkey = saf.lionkey AND saf.is_written
    CROSS JOIN LATERAL generate_series(1, CASE WHEN saf.parity_side IS NOT NULL THEN 2 ELSE 1 END) AS rng (n)
    CROSS JOIN LATERAL (
        {{ lion_house_number_split(
            'saf.l_low_hn', 'saf.l_high_hn', 'saf.r_low_hn', 'saf.r_high_hn',
            'saf.parity_side', 'rng.n', 'TRUE'
        ) }}
    ) AS hn
),
changed AS (
    SELECT record_type
    FROM records
    WHERE coalesce(l_low, l_high, r_low, r_high) IS NOT NULL
)
SELECT
    'warning' AS log_level,
    'house number zeroing' AS error_category,
    'CSCL-LOG-OURS-15' AS log_message_id,
    NULL::VARCHAR AS globalid,
    NULL::VARCHAR AS source_feature_layer,
    NULL::VARCHAR AS record_id_type,
    NULL::VARCHAR AS record_id,
    {% if var('zero_generic_house_numbers') -%}
    {{ log_message(
        "{} LION gdb records of generic segments ({} SAF replicants) have house numbers. "
        "Zeroing generic segments' house numbers is ON in this build (zero_generic_house_numbers = true): "
        "they are zeroed, as the spec and the legacy tool's rule do, where prod's LION gdb keeps them (CSCL-LION-15)",
        ["count(*)", "count(*) FILTER (WHERE record_type = 'saf_replicant')"]
    ) }}
    {%- else -%}
    {{ log_message(
        "{} LION gdb records of generic segments ({} SAF replicants) have house numbers. "
        "Zeroing generic segments' house numbers is OFF in this build (zero_generic_house_numbers = false): "
        "they keep them, as prod's LION gdb does, where the spec and the legacy tool's rule zero them (CSCL-LION-15)",
        ["count(*)", "count(*) FILTER (WHERE record_type = 'saf_replicant')"]
    ) }}
    {%- endif %} AS message,
    count(*) AS record_count,
    NULL::VARCHAR AS output_filename
FROM changed
