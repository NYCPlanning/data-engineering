-- depends_on: {{ ref('log_records') }}, {{ ref('int__lion') }}, {{ ref('int__saf_gnx') }}, {{ ref('int__saf_abcep') }}, {{ ref('int__snd') }}, {{ ref('enders_by_field') }}, {{ ref('int__rpl') }}, {{ ref('int__saf_s') }}, {{ ref('int__saf_v') }}, {{ ref('int__saf_d') }}, {{ ref('int__saf_o') }}, {{ ref('int__saf_replicant_fields') }}, {{ ref('gdb_lion') }}, {{ ref('log_messages') }}
-- One row per chaos injection and expected log id (poc_validation/chaos_monkey.py writes
-- the chaos_injections manifest): was the injected record logged under that id, and is a
-- record the message says is dropped really missing from its output. Empty unless the
-- manifest exists, so a normal build skips it.
{% set manifest = adapter.get_relation(
    database = target.database,
    schema = target.schema,
    identifier = "chaos_injections"
) if execute else none -%}

{% if manifest %}
    WITH injections AS (
        SELECT * FROM {{ manifest }}
    ),
    log AS (
        SELECT * FROM {{ ref('log_records') }}
    ),
    lion AS (
        SELECT DISTINCT globalid FROM {{ ref('int__lion') }}
    ),
    saf AS (
        SELECT saf_globalid AS globalid FROM {{ ref('int__saf_gnx') }}
        UNION
        SELECT saf_globalid AS globalid FROM {{ ref('int__saf_abcep') }}
        -- S, V, D and O records with no segment type stay in their models but reach
        -- neither the generic nor the roadbed file
        UNION
        SELECT saf_globalid AS globalid FROM {{ ref('int__saf_s') }}
        WHERE generic OR roadbed
        UNION
        SELECT saf_globalid AS globalid FROM {{ ref('int__saf_v') }}
        WHERE generic OR roadbed
        UNION
        SELECT saf_globalid AS globalid FROM {{ ref('int__saf_d') }}
        WHERE generic OR roadbed
        UNION
        SELECT saf_globalid AS globalid FROM {{ ref('int__saf_o') }}
        WHERE generic OR roadbed
    ),
    snd AS (
        SELECT DISTINCT globalid FROM {{ ref('int__snd') }}
    ),
    rpl AS (
        SELECT DISTINCT globalid FROM {{ ref('int__rpl') }}
    ),
    enders AS (
        SELECT DISTINCT _globalid AS globalid FROM {{ ref('enders_by_field') }}
    ),
    lion_saf AS (
        SELECT DISTINCT saf_globalid AS globalid FROM {{ ref('int__saf_replicant_fields') }}
        WHERE is_written
    ),
    -- a feature with any LION gdb row on its segment
    gdb_lion AS (
        SELECT DISTINCT int__lion.globalid
        FROM {{ ref('int__lion') }} AS int__lion
        INNER JOIN {{ ref('gdb_lion') }} AS gdb_lion
            ON LPAD(int__lion.segmentid::VARCHAR, 7, '0') = gdb_lion."SegmentID"
        WHERE int__lion.globalid IN (
            SELECT dropped_globalid FROM injections WHERE dropped_from = 'gdb_lion'
        )
    ),
    found AS (
        SELECT DISTINCT
            injections.injection_id,
            injections.log_message_id
        FROM injections
        INNER JOIN log
            ON
                injections.log_message_id = log.log_message_id
                AND CASE injections.log_match_column
                    WHEN 'globalid' THEN injections.log_match_value = log.globalid
                    WHEN 'message' THEN CONTAINS(log.message, injections.log_match_value)
                END
    )
    SELECT
        injections.*,
        log_messages.status AS log_message_status,
        found.injection_id IS NOT NULL AS logged,
        CASE injections.dropped_from
            WHEN 'lion' THEN lion.globalid IS NULL
            WHEN 'saf' THEN saf.globalid IS NULL
            WHEN 'snd' THEN snd.globalid IS NULL
            WHEN 'rpl' THEN rpl.globalid IS NULL
            WHEN 'enders' THEN enders.globalid IS NULL
            WHEN 'lion_saf' THEN lion_saf.globalid IS NULL
            WHEN 'gdb_lion' THEN gdb_lion.globalid IS NULL
        END AS dropped
    FROM injections
    LEFT JOIN {{ ref('log_messages') }} AS log_messages ON injections.log_message_id = log_messages.id
    LEFT JOIN found
        ON injections.injection_id = found.injection_id AND injections.log_message_id = found.log_message_id
    LEFT JOIN lion ON injections.dropped_from = 'lion' AND injections.dropped_globalid = lion.globalid
    LEFT JOIN saf ON injections.dropped_from = 'saf' AND injections.dropped_globalid = saf.globalid
    LEFT JOIN snd ON injections.dropped_from = 'snd' AND injections.dropped_globalid = snd.globalid
    LEFT JOIN rpl ON injections.dropped_from = 'rpl' AND injections.dropped_globalid = rpl.globalid
    LEFT JOIN enders ON injections.dropped_from = 'enders' AND injections.dropped_globalid = enders.globalid
    LEFT JOIN lion_saf ON injections.dropped_from = 'lion_saf' AND injections.dropped_globalid = lion_saf.globalid
    LEFT JOIN gdb_lion ON injections.dropped_from = 'gdb_lion' AND injections.dropped_globalid = gdb_lion.globalid
{% else %}
    SELECT
        NULL::INT AS injection_id,
        NULL::VARCHAR AS mutation,
        NULL::VARCHAR AS log_message_id,
        NULL::VARCHAR AS expected_ids,
        NULL::VARCHAR AS table_name,
        NULL::VARCHAR AS key_column,
        NULL::VARCHAR AS key_value,
        NULL::VARCHAR AS column_name,
        NULL::VARCHAR AS old_value,
        NULL::VARCHAR AS new_value,
        NULL::VARCHAR AS log_match_column,
        NULL::VARCHAR AS log_match_value,
        NULL::VARCHAR AS dropped_from,
        NULL::VARCHAR AS dropped_globalid,
        NULL::BOOLEAN AS required,
        NULL::VARCHAR AS log_message_status,
        NULL::BOOLEAN AS logged,
        NULL::BOOLEAN AS dropped
    WHERE FALSE
{% endif %}
