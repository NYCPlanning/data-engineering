-- SAF records left out because their SEGMENTID matches no segment (int__saf_abcep and
-- int__saf_gnx drop them), in prod's "segment feature is NULL" wording.
SELECT
    'error' AS log_level,
    'saf record segment not found' AS error_category,
    log_message_id,
    globalid,
    source_table AS source_feature_layer,
    CASE source_table WHEN 'commonplace' THEN 'placeid' ELSE 'segmentid' END AS record_id_type,
    CASE source_table WHEN 'commonplace' THEN placeid ELSE segmentid::VARCHAR END AS record_id,
    CASE source_table
        WHEN 'commonplace'
            THEN {{ log_message(
                "Failed to add [SAFA] record from [CommonPlace] featureclass with SEGMENTID = [{}] "
                "and PLACEID = [{}] as segment feature is NULL",
                ["segmentid", "placeid"]
            ) }}
        ELSE {{ log_message(
            "Failed to add [SAFA] record from [AltSegmentData] table with SEGMENTID = [{}] "
            "and LIONKEY = [{}] as segment feature is NULL",
            ["segmentid", "lionkey"]
        ) }}
    END AS message
FROM {{ ref('int__saf_orphan_segmentids') }}
