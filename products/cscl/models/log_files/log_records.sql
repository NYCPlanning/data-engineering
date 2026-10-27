-- Every logged record: log.csv's rows, with the per-borough FC-01 counts swapped for the
-- records behind them (log_face_code_missing.csv). What the chaos QA models check.
SELECT
    log_level,
    error_category,
    log_message_id,
    effect,
    globalid,
    source_feature_layer,
    record_id_type,
    record_id,
    message
FROM {{ ref('log') }}
WHERE log_message_id <> 'CSCL-LOG-FC-01'
UNION ALL
SELECT
    log_level,
    error_category,
    log_message_id,
    effect,
    globalid,
    source_feature_layer,
    record_id_type,
    record_id,
    message
FROM {{ ref('log_face_code_missing') }}
