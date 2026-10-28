-- Attachment to log.csv, exported as log_boe_lgc_pointer.csv: every LION record (LION .dat
-- or LION gdb) whose BOE LGC pointer differs between prod's rule and the fixed one
-- (CSCL-LION-04): its BOE-preferred LGC value is above 9 and isn't its first LGC. Prod
-- (CenterlineExtractFile.cs:1321) writes 1; the fix writes the LGC's position. Which one
-- the build writes is the var fix_boe_lgc_pointer (default false: prod's). Logged every
-- build either way, as CSCL-LOG-OURS-06; log.csv carries the count (log__boe_lgc_pointer).
-- Proto segments take their pointer from AltSegmentData as is and aren't affected.
WITH lion AS (
    SELECT * FROM {{ ref('int__lion') }}
    WHERE
        source_table = 'centerline'
        AND (include_in_geosupport_lion OR include_in_bytes_lion)
),
pointers AS (
    SELECT * FROM {{ ref('int__streetcode_and_facecode') }}
    WHERE boe_lgc_pointer_prod IS DISTINCT FROM boe_lgc_pointer_fixed
)
SELECT
    'warning' AS log_level,
    'boe lgc pointer legacy bug' AS error_category,
    'CSCL-LOG-OURS-06' AS log_message_id,
    log_messages.effect,
    lion.globalid,
    lion.source_table AS source_feature_layer,
    'segmentid' AS record_id_type,
    lion.segmentid::VARCHAR AS record_id,
    lion.boroughcode,
    lion.face_code,
    lion.five_digit_street_code,
    pointers.boe_preferred_lgc,
    pointers.boe_lgc_pointer_prod,
    pointers.boe_lgc_pointer_fixed,
    lion.boe_lgc_pointer AS boe_lgc_pointer_written,
    '[CSCL-LOG-OURS-06] ' || {{ log_message(
        "The centerline feature with a GlobalID = {} (SegmentID = {}) has BOE-preferred "
        "LGC {} at LGC position {}. Prod writes BOE LGC pointer {}, the fix writes {}; "
        "this build writes {} (fix_boe_lgc_pointer = "
        ~ ("true" if var("fix_boe_lgc_pointer") else "false") ~ ").",
        [
            "lion.globalid", "lion.segmentid", "pointers.boe_preferred_lgc",
            "pointers.boe_lgc_pointer_fixed", "pointers.boe_lgc_pointer_prod",
            "pointers.boe_lgc_pointer_fixed", "lion.boe_lgc_pointer",
        ]
    ) }} AS message
FROM lion
INNER JOIN pointers ON lion.segmentid = pointers.segmentid
LEFT JOIN {{ ref('log_messages') }} AS log_messages ON log_messages.id = 'CSCL-LOG-OURS-06'
ORDER BY lion.boroughcode, lion.segmentid, lion.five_digit_street_code
