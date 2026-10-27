WITH segments AS (
    SELECT
        {{ dbt_utils.star(ref('int__primary_segments')) }}
    FROM {{ ref("int__primary_segments") }}
    UNION ALL
    SELECT
        {{ dbt_utils.star(ref('int__primary_segments')) }}
    FROM {{ ref("int__protosegments") }}
)
SELECT DISTINCT
    'error' AS log_level,
    'segment missing facecode' AS error_category,
    CASE WHEN source_table = 'altsegmentdata' THEN 'CSCL-LOG-LION-19' ELSE 'CSCL-LOG-LION-18' END
        AS log_message_id,
    'dropped' AS effect,
    globalid,
    source_table AS source_feature_layer,
    'segmentid' AS record_id_type,
    segmentid AS record_id,
    CASE
        WHEN source_table = 'altsegmentdata'
            THEN FORMAT(
                '[CSCL-LOG-LION-19] Unable to obtain a FaceCode value for the proto segment with a '
                || 'GlobalID = {}.  The proto segment record will not be included in the LION extract file.',
                COALESCE(globalid::VARCHAR, '')
            )
        ELSE FORMAT(
            '[CSCL-LOG-LION-18] Unable to obtain a FaceCode value for the {} feature a GlobalID = {}.  '
            || 'The feature record will not be included in the LION extract file.',
            COALESCE(source_table::VARCHAR, ''),
            COALESCE(globalid::VARCHAR, '')
        )
    END AS message
FROM segments
WHERE face_code IS NULL
