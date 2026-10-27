-- Centerline, rail and subway FROM_/TO_LEVEL_CODE values the LION level code can't be
-- made from. Prod (GetLevelCode) skips non-street features and shoreline, and logs once
-- per feature and field: a blank value as "[EMPTY LEVEL CODE]" (CSCL-LOG-LION-10), a
-- number other than 1-26 or 99 as "[NO MATCHING LEVEL CODE]" (CSCL-LOG-LION-12). Both
-- leave the level code blank, as convert_level_code does. Proto segments copy their
-- centerline's level codes and aren't checked again.
WITH features AS (
    SELECT
        source_table,
        globalid,
        segmentid,
        from_level_code,
        to_level_code
    FROM {{ ref('stg__centerline') }}
    UNION ALL
    SELECT
        source_table,
        globalid,
        segmentid,
        from_level_code,
        to_level_code
    FROM {{ ref('stg__rail_and_subway') }}
),
level_codes AS (
    SELECT
        source_table,
        globalid,
        segmentid,
        'FROM_LEVEL_CODE' AS field_name,
        from_level_code AS level_code
    FROM features
    UNION ALL
    SELECT
        source_table,
        globalid,
        segmentid,
        'TO_LEVEL_CODE' AS field_name,
        to_level_code AS level_code
    FROM features
)
SELECT
    'warning' AS log_level,
    'empty level code' AS error_category,
    'CSCL-LOG-LION-10' AS log_message_id,
    globalid,
    source_table AS source_feature_layer,
    'segmentid' AS record_id_type,
    segmentid AS record_id,
    {{ log_message(
        "[EMPTY LEVEL CODE] The {} field in the {} feature with a GlobalID = {} has an empty value.",
        ["field_name", "source_table", "globalid"]
    ) }} AS message
FROM level_codes
WHERE level_code IS NULL
UNION ALL
SELECT
    'warning' AS log_level,
    'invalid level code' AS error_category,
    'CSCL-LOG-LION-12' AS log_message_id,
    globalid,
    source_table AS source_feature_layer,
    'segmentid' AS record_id_type,
    segmentid AS record_id,
    {{ log_message(
        "[NO MATCHING LEVEL CODE] The {} field in the {} feature with a GlobalID = {} "
        "does not appear to have a valid value ({}).",
        ["field_name", "source_table", "globalid", "level_code"]
    ) }} AS message
FROM level_codes
WHERE NOT (level_code BETWEEN 1 AND 26 OR level_code = 99)
