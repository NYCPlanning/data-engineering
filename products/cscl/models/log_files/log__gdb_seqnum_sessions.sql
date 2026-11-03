-- CSCL-LOG-OURS-22, every build: the extract sessions the LION gdb and the LION .dat files
-- number non-street features' sequence numbers in (vars lion_gdb_seqnum_extract_sessions and
-- lion_seqnum_extract_sessions, CSCL-LION-05), and how many LION gdb records get a different
-- SeqNum from the .dat files' numbering. Records are counted as gdb_lion writes them: two for
-- a continuous-parity segment or SAF entry, its SAF replicants included. One row, also when
-- the count is 0.
{%- set ns = namespace(gdb='', dat='') -%}
{%- for session in var('lion_gdb_seqnum_extract_sessions') -%}
    {%- set ns.gdb = ns.gdb ~ ('' if loop.first else ' ') ~ '[' ~ (session | join(', ')) ~ ']' -%}
{%- endfor -%}
{%- for session in var('lion_seqnum_extract_sessions') -%}
    {%- set ns.dat = ns.dat ~ ('' if loop.first else ' ') ~ '[' ~ (session | join(', ')) ~ ']' -%}
{%- endfor %}
WITH changed_segments AS (
    SELECT lion.*
    FROM {{ ref('int__lion') }} AS lion
    WHERE
        lion.include_in_bytes_lion
        AND lion.gdb_segment_seqnum IS DISTINCT FROM lion.segment_seqnum
        -- gdb_lion writes no record of a segment whose Street is blank
        AND trim(coalesce(lion.street, '')) <> ''
),
records AS (
    SELECT CASE WHEN continuous_parity_flag IN ('L', 'R') THEN 2 ELSE 1 END AS n_records
    FROM changed_segments
    UNION ALL
    SELECT CASE WHEN saf.parity_side IS NOT NULL THEN 2 ELSE 1 END AS n_records
    FROM changed_segments AS lion
    INNER JOIN {{ ref('int__saf_replicant_fields') }} AS saf
        ON lion.lionkey = saf.lionkey AND saf.is_written
)
SELECT
    'warning' AS log_level,
    'sequence number sessions' AS error_category,
    'CSCL-LOG-OURS-22' AS log_message_id,
    NULL::VARCHAR AS globalid,
    NULL::VARCHAR AS source_feature_layer,
    NULL::VARCHAR AS record_id_type,
    NULL::VARCHAR AS record_id,
    {{ log_message(
        "{} LION gdb records have a different non-street feature SeqNum from the LION .dat files' numbering. "
        "This build numbers the LION gdb in the extract sessions " ~ ns.gdb
        ~ " (lion_gdb_seqnum_extract_sessions) and the LION .dat files in " ~ ns.dat
        ~ " (lion_seqnum_extract_sessions); prod's LION gdb matches one session for all five boroughs "
        "and its .dat files a fresh session for Queens and Staten Island (CSCL-LION-05)",
        ["coalesce(sum(n_records), 0)"]
    ) }} AS message,
    coalesce(sum(n_records), 0) AS record_count,
    NULL::VARCHAR AS output_filename
FROM records
