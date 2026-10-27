-- RoadbedPointerList rows prod (RPLExtractClass.cs:88-207) leaves out of RPL because a
-- centerline they point to is missing (or has an empty shape): the generic one
-- (CSCL-LOG-RPL-06), else the roadbed one (CSCL-LOG-RPL-05).
WITH rpl AS (
    SELECT * FROM {{ source('recipe_sources', 'dcp_cscl_roadbed_pointer_list') }}
),
centerline AS (
    SELECT DISTINCT segmentid
    FROM {{ source('recipe_sources', 'dcp_cscl_centerline') }}
    WHERE geom IS NOT NULL AND NOT ST_ISEMPTY(geom)
),
missing AS (
    SELECT
        rpl.*,
        CASE
            WHEN generic.segmentid IS NULL THEN 'CSCL-LOG-RPL-06'
            WHEN roadbed.segmentid IS NULL THEN 'CSCL-LOG-RPL-05'
        END AS log_message_id
    FROM rpl
    LEFT JOIN centerline AS generic ON rpl.generic_segmentid = generic.segmentid
    LEFT JOIN centerline AS roadbed ON rpl.roadbed_segmentid = roadbed.segmentid
)
SELECT
    'error' AS log_level,
    CASE log_message_id
        WHEN 'CSCL-LOG-RPL-06' THEN 'generic centerline missing'
        ELSE 'roadbed centerline missing'
    END AS error_category,
    log_message_id,
    globalid,
    'roadbed_pointer_list' AS source_feature_layer,
    'roadbed_segmentid' AS record_id_type,
    roadbed_segmentid AS record_id,
    {{ log_message(
        "Failed to add [RPL] record from [RoadbedPointerList] table with "
        "GENERIC_SEGMENTID = [{}] and ROADBED_SEGMENTID = [{}] as {} centerline "
        "feature is NULL",
        [
            "generic_segmentid",
            "roadbed_segmentid",
            "CASE log_message_id WHEN 'CSCL-LOG-RPL-06' THEN 'generic' ELSE 'roadbed' END",
        ]
    ) }} AS message
FROM missing
WHERE log_message_id IS NOT NULL
