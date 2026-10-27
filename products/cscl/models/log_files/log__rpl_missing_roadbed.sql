-- RoadbedPointerList rows whose ROADBED_SEGMENTID matches no centerline (or one with an
-- empty shape), while the generic segment is there. Prod (RPLExtractClass) leaves the
-- row out of RPL and logs it as CSCL-LOG-RPL-05; a missing generic centerline is logged
-- under CSCL-LOG-RPL-06 instead.
WITH rpl AS (
    SELECT * FROM {{ source('recipe_sources', 'dcp_cscl_roadbed_pointer_list') }}
),
centerline AS (
    SELECT DISTINCT segmentid
    FROM {{ source('recipe_sources', 'dcp_cscl_centerline') }}
    WHERE geom IS NOT NULL AND NOT ST_ISEMPTY(geom)
)
SELECT
    'error' AS log_level,
    'roadbed centerline missing' AS error_category,
    'CSCL-LOG-RPL-05' AS log_message_id,
    rpl.globalid,
    'roadbed_pointer_list' AS source_feature_layer,
    'roadbed_segmentid' AS record_id_type,
    rpl.roadbed_segmentid AS record_id,
    {{ log_message(
        "Failed to add [RPL] record from [RoadbedPointerList] table with "
        "GENERIC_SEGMENTID = [{}] and ROADBED_SEGMENTID = [{}] as roadbed centerline "
        "feature is NULL",
        ["rpl.generic_segmentid", "rpl.roadbed_segmentid"]
    ) }} AS message
FROM rpl
INNER JOIN centerline AS generic ON rpl.generic_segmentid = generic.segmentid
LEFT JOIN centerline AS roadbed ON rpl.roadbed_segmentid = roadbed.segmentid
WHERE roadbed.segmentid IS NULL
