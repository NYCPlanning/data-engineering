-- Every logged record: log.csv's rows, with the per-borough FC-01, FC-03 and BYTES-06
-- counts, the per-name name-table warning counts and the OURS-06 count swapped for the
-- records behind them (log_face_code_missing.csv, log_altnames_sname_cut.csv,
-- log_name_table_warnings.csv, log_boe_lgc_pointer.csv).
-- What the chaos QA models check.
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
WHERE
    log_message_id NOT IN (
        'CSCL-LOG-FC-01', 'CSCL-LOG-FC-03', 'CSCL-LOG-BYTES-06',
        -- log__name_table_warnings' per-name counts
        'CSCL-LOG-LION-13', 'CSCL-LOG-LION-14', 'CSCL-LOG-LION-15', 'CSCL-LOG-LION-16',
        'CSCL-LOG-BYTES-05', 'CSCL-LOG-BYTES-08', 'CSCL-LOG-BYTES-09', 'CSCL-LOG-SAF-19', 'CSCL-LOG-SAF-24',
        -- log__boe_lgc_pointer's count
        'CSCL-LOG-OURS-06'
    )
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
FROM {{ ref('log_name_table_warnings') }}
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
FROM {{ ref('log_altnames_sname_cut') }}
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
FROM {{ ref('log_boe_lgc_pointer') }}
