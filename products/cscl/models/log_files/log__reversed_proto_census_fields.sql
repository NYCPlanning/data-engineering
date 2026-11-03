-- CSCL-LOG-OURS-17, every build: how many LION gdb records of a centerline's proto-segments
-- (their own records and their SAF replicants) prod writes with the census tract and block
-- and community district fields of the other side, because its gdb reads them from the
-- shape before the proto's reversal (CSCL-LION-26, var fix_reversed_proto_census_fields),
-- and whether the fix is on in this build. Only records whose two sides differ in one of
-- those fields count. One row, also when the count is 0.
WITH protos AS (
    SELECT lion.lionkey
    FROM {{ ref('int__lion') }} AS lion
    INNER JOIN {{ ref('int__protosegments') }} AS protos
        ON lion.globalid = protos.globalid
    WHERE
        lion.include_in_bytes_lion
        AND lion.source_table = 'altsegmentdata'
        -- gdb_lion writes no record of a segment whose Street is blank
        AND trim(coalesce(lion.street, '')) <> ''
        AND protos.gdb_census_sides_swapped_prod IS DISTINCT FROM protos.gdb_census_sides_swapped_fixed
        AND {{ lion_gdb_census_sides_differ('lion') }}
),
changed AS (
    SELECT 'segment' AS record_type
    FROM protos
    UNION ALL
    SELECT 'saf_replicant' AS record_type
    FROM protos
    INNER JOIN {{ ref('int__saf_replicant_fields') }} AS saf
        ON protos.lionkey = saf.lionkey AND saf.is_written
)
SELECT
    'warning' AS log_level,
    'reversed proto legacy bug' AS error_category,
    'CSCL-LOG-OURS-17' AS log_message_id,
    NULL::VARCHAR AS globalid,
    'altsegmentdata' AS source_feature_layer,
    NULL::VARCHAR AS record_id_type,
    NULL::VARCHAR AS record_id,
    {% if var('fix_reversed_proto_census_fields') -%}
    {{ log_message(
        "{} LION gdb records of reversed proto-segments ({} SAF replicants) have census tract, block or "
        "community district fields that differ between their sides. The reversed proto census fix is ON in this "
        "build (fix_reversed_proto_census_fields = true): they follow the record's own sides, as the LION .dat, "
        "where prod's gdb writes the other side's (CSCL-LION-26)",
        ["count(*)", "count(*) FILTER (WHERE record_type = 'saf_replicant')"]
    ) }}
    {%- else -%}
    {{ log_message(
        "{} LION gdb records of reversed proto-segments ({} SAF replicants) have census tract, block or "
        "community district fields that differ between their sides. The reversed proto census fix is OFF in this "
        "build (fix_reversed_proto_census_fields = false): they are the other side's, as prod's gdb writes them, "
        "where the LION .dat has the record's own (CSCL-LION-26)",
        ["count(*)", "count(*) FILTER (WHERE record_type = 'saf_replicant')"]
    ) }}
    {%- endif %} AS message,
    count(*) AS record_count,
    NULL::VARCHAR AS output_filename
FROM changed
