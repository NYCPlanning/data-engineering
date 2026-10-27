-- Records whose name prod finds only in the other name table (CSCL-LOG-LION-13,
-- CSCL-LOG-LION-14, CSCL-LOG-LION-15, CSCL-LOG-LION-16, CSCL-LOG-BYTES-05, CSCL-LOG-BYTES-08,
-- CSCL-LOG-BYTES-09, CSCL-LOG-SAF-19, CSCL-LOG-SAF-24): prod warns once per record, and the same place repeats
-- hundreds of times, so log.csv gets one row per id and name with the count in
-- record_count. The records are in log_name_table_warnings.csv (log_name_table_warnings).
SELECT
    'warning' AS log_level,
    'name table warning' AS error_category,
    log_message_id,
    NULL AS globalid,
    STRING_AGG(DISTINCT source_feature_layer, ', ' ORDER BY source_feature_layer) AS source_feature_layer,
    'b7sc' AS record_id_type,
    MIN(b7sc) AS record_id,
    {{ log_message(
        "{} {} record(s) are named {}, which is in the {} table, not in the {} table; "
        "see log_name_table_warnings.csv",
        [
            "COUNT(*)",
            "STRING_AGG(DISTINCT source_feature_layer, ', ' ORDER BY source_feature_layer)",
            "name",
            "CASE WHEN log_message_id IN ('CSCL-LOG-LION-14', 'CSCL-LOG-LION-16', 'CSCL-LOG-BYTES-09', "
            ~ "'CSCL-LOG-SAF-24') THEN 'StreetName' ELSE 'FeatureName' END",
            "CASE WHEN log_message_id IN ('CSCL-LOG-LION-14', 'CSCL-LOG-LION-16', 'CSCL-LOG-BYTES-09', "
            ~ "'CSCL-LOG-SAF-24') THEN 'FeatureName' ELSE 'StreetName' END",
        ]
    ) }} AS message,
    COUNT(*) AS record_count,
    NULL AS output_filename
FROM {{ ref('log_name_table_warnings') }}
GROUP BY log_message_id, name
