-- SAF records left out because their SEGMENTID matches no segment, in prod's "segment
-- feature is NULL" wording (int__saf_orphan_segmentids). int__saf_abcep and int__saf_gnx
-- drop them; an S, V, D/F or O record with no segment has no segment type, so it reaches
-- neither the generic nor the roadbed SAF file.
SELECT
    'error' AS log_level,
    'saf record segment not found' AS error_category,
    log_message_id,
    globalid,
    source_table AS source_feature_layer,
    CASE source_table WHEN 'commonplace' THEN 'placeid' ELSE 'segmentid' END AS record_id_type,
    CASE source_table WHEN 'commonplace' THEN placeid ELSE segmentid::VARCHAR END AS record_id,
    CASE log_message_id
        WHEN 'CSCL-LOG-SAF-25'
            THEN {{ log_message(
                "Failed to add [SAFA] record from [CommonPlace] featureclass with SEGMENTID = [{}] "
                "and PLACEID = [{}] as segment feature is NULL",
                ["segmentid", "placeid"]
            ) }}
        WHEN 'CSCL-LOG-SAF-12'
            THEN {{ log_message(
                "Failed to add [SAFS] record from [AddressPoint] featureclass with SEGMENTID = [{}] "
                "and ADDRESSPOINTID = [{}] as segment feature is NULL",
                ["segmentid", "addresspointid"]
            ) }}
        WHEN 'CSCL-LOG-SAF-16'
            THEN {{ log_message(
                "Failed to add [SAFV] record from [AddressPoint] featureclass with SEGMENTID = [{}] "
                "and ADDRESSPOINTID = [{}] as segment feature is NULL",
                ["segmentid", "addresspointid"]
            ) }}
        WHEN 'CSCL-LOG-SAF-15'
            THEN {{ log_message(
                "Failed to add [SAFD] record from [AltSegmentData] table with SEGMENTID = [{}] "
                "AND LIONKEY = [{}] as segment feature is NULL",
                ["segmentid", "lionkey"]
            ) }}
        WHEN 'CSCL-LOG-SAF-18'
            THEN {{ log_message(
                "Failed to add [SAFV] record from [AltSegmentData] table with SEGMENTID = [{}] "
                "AND LIONKEY = [{}] as segment feature is NULL",
                ["segmentid", "lionkey"]
            ) }}
        -- CSCL-LOG-SAF-21
        ELSE {{ log_message(
            "Failed to add [SAFA] record from [AltSegmentData] table with SEGMENTID = [{}] "
            "and LIONKEY = [{}] as segment feature is NULL",
            ["segmentid", "lionkey"]
        ) }}
    END AS message
FROM {{ ref('int__saf_orphan_segmentids') }}
