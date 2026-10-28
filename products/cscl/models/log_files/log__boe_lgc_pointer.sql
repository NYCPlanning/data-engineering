-- CSCL-LOG-OURS-06, every build: how many LION records the BOE LGC pointer fix
-- (fix_boe_lgc_pointer, CSCL-LION-04) changes, and whether it is on in this build. One
-- row, also when the count is 0; the records are in log_boe_lgc_pointer.csv.
SELECT
    'warning' AS log_level,
    'boe lgc pointer legacy bug' AS error_category,
    'CSCL-LOG-OURS-06' AS log_message_id,
    NULL AS globalid,
    'centerline' AS source_feature_layer,
    NULL AS record_id_type,
    NULL AS record_id,
    {% if var('fix_boe_lgc_pointer') -%}
    {{ log_message(
        "{} LION records have a BOE-preferred LGC above 9 that isn't their first LGC. "
        "The BOE LGC pointer fix is ON in this build (fix_boe_lgc_pointer = true): they get "
        "that LGC's position, where prod writes 1 (CSCL-LION-04); see log_boe_lgc_pointer.csv",
        ["COUNT(*)"]
    ) }}
    {%- else -%}
    {{ log_message(
        "{} LION records have a BOE-preferred LGC above 9 that isn't their first LGC. "
        "The BOE LGC pointer fix is OFF in this build (fix_boe_lgc_pointer = false): they get "
        "pointer 1, as prod, not that LGC's position (CSCL-LION-04); see log_boe_lgc_pointer.csv",
        ["COUNT(*)"]
    ) }}
    {%- endif %} AS message,
    COUNT(*) AS record_count,
    NULL AS output_filename
FROM {{ ref('log_boe_lgc_pointer') }}
