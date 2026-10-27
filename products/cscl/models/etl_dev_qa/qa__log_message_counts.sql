-- log.csv rows per log_messages id, with every seed row listed (0 when a build logs none).
-- record_count sums the ThinLION lookup rows' atomic-polygon counts, so it's comparable to
-- prod's line count for those ids; elsewhere it's the row count.
WITH log_counts AS (
    SELECT
        log_message_id,
        COUNT(*) AS log_rows,
        SUM(
            CASE WHEN error_category = 'thinlion lookup not found' THEN record_count ELSE 1 END
        ) AS records
    FROM {{ ref('log') }}
    GROUP BY log_message_id
)
SELECT
    lm.id AS log_message_id,
    lm.level,
    lm.effect,
    lm.status,
    lm.model,
    COALESCE(log_counts.log_rows, 0) AS log_rows,
    COALESCE(log_counts.records, 0) AS records
FROM {{ ref('log_messages') }} AS lm
LEFT JOIN log_counts ON lm.id = log_counts.log_message_id
ORDER BY lm.id
