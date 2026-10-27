-- Chaos run result per log id. recall: injections logged under the id. precision: log rows
-- with the id that are new since the baseline run (chaos_baseline_log, the log table
-- chaos_monkey.py found before it injected) and match no injection; NULL without a
-- baseline. effect: dropped records still in their output. Summary rows are left out of
-- precision, since dropping records changes their counts.
{% set baseline = adapter.get_relation(
    database = target.database,
    schema = target.schema,
    identifier = "chaos_baseline_log"
) if execute else none -%}

WITH injections AS (
    SELECT * FROM {{ ref('qa__chaos_injections') }}
),
log AS (
    SELECT * FROM {{ ref('log') }}
    WHERE effect <> 'summary'
),
{% if baseline %}
    new_rows AS (
        SELECT
            log_message_id,
            globalid,
            message
        FROM log
        EXCEPT
        SELECT
            log_message_id,
            globalid,
            message
        FROM {{ baseline }}
    ),
    unexpected AS (
        SELECT
            new_rows.log_message_id,
            COUNT(*) AS unexpected_rows
        FROM new_rows
        WHERE NOT EXISTS (
            SELECT 1 FROM injections
            WHERE
                injections.log_message_id = new_rows.log_message_id
                AND CASE injections.log_match_column
                    WHEN 'globalid' THEN new_rows.globalid = injections.log_match_value
                    WHEN 'message' THEN CONTAINS(new_rows.message, injections.log_match_value)
                END
        )
        GROUP BY new_rows.log_message_id
    ),
{% else %}
    unexpected AS (
        SELECT
            NULL::VARCHAR AS log_message_id,
            NULL::BIGINT AS unexpected_rows
        WHERE FALSE
    ),
{% endif %}
by_id AS (
    SELECT
        log_message_id,
        ANY_VALUE(log_message_status) AS log_message_status,
        COUNT(*) AS injected,
        COUNT(*) FILTER (WHERE logged) AS logged,
        COUNT(dropped) AS dropped_checked,
        COUNT(*) FILTER (WHERE NOT dropped) AS not_dropped,
        STRING_AGG(DISTINCT mutation, ', ') AS mutations
    FROM injections
    GROUP BY log_message_id
)
SELECT
    COALESCE(by_id.log_message_id, unexpected.log_message_id) AS log_message_id,
    COALESCE(by_id.log_message_status, log_messages.status) AS log_message_status,
    COALESCE(by_id.injected, 0) AS injected,
    COALESCE(by_id.logged, 0) AS logged,
    COALESCE(by_id.dropped_checked, 0) AS dropped_checked,
    COALESCE(by_id.not_dropped, 0) AS not_dropped,
    {% if baseline %}COALESCE(unexpected.unexpected_rows, 0){% else %}NULL::BIGINT{% endif %}
        AS unexpected_rows,
    CASE
        WHEN
            COALESCE(by_id.logged, 0) = COALESCE(by_id.injected, 0)
            AND COALESCE(by_id.not_dropped, 0) = 0
            AND COALESCE(unexpected.unexpected_rows, 0) = 0
            THEN 'pass'
        WHEN COALESCE(by_id.log_message_status, log_messages.status) = 'planned' THEN 'expected fail'
        ELSE 'fail'
    END AS result,
    by_id.mutations
FROM by_id
FULL OUTER JOIN unexpected ON by_id.log_message_id = unexpected.log_message_id
LEFT JOIN {{ ref('log_messages') }} AS log_messages
    ON COALESCE(by_id.log_message_id, unexpected.log_message_id) = log_messages.id
ORDER BY log_message_id
