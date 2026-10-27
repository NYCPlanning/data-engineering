-- depends_on: {{ ref('log') }}, {{ ref('int__lion') }}, {{ ref('int__saf_gnx') }}, {{ ref('int__saf_abcep') }}, {{ ref('log_messages') }}
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
        SELECT * FROM {{ ref('log') }}
    ),
    lion AS (
        SELECT DISTINCT globalid FROM {{ ref('int__lion') }}
    ),
    saf AS (
        SELECT saf_globalid AS globalid FROM {{ ref('int__saf_gnx') }}
        UNION
        SELECT saf_globalid AS globalid FROM {{ ref('int__saf_abcep') }}
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
                    WHEN 'globalid' THEN log.globalid = injections.log_match_value
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
        END AS dropped
    FROM injections
    LEFT JOIN {{ ref('log_messages') }} AS log_messages ON injections.log_message_id = log_messages.id
    LEFT JOIN found
        ON injections.injection_id = found.injection_id AND injections.log_message_id = found.log_message_id
    LEFT JOIN lion ON injections.dropped_from = 'lion' AND injections.dropped_globalid = lion.globalid
    LEFT JOIN saf ON injections.dropped_from = 'saf' AND injections.dropped_globalid = saf.globalid
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
        NULL::VARCHAR AS log_message_status,
        NULL::BOOLEAN AS logged,
        NULL::BOOLEAN AS dropped
    WHERE FALSE
{% endif %}
