-- G/N/X CommonPlaces prod leaves out of SAF while looking up their face code and sequence
-- number (int__saf_commonplace_proto, which int__saf_gnx drops).
WITH failed AS (
    SELECT * FROM {{ ref('int__saf_commonplace_proto') }}
)
SELECT
    'error' AS log_level,
    'saf place segment not matched' AS error_category,
    log_message_id,
    globalid,
    'commonplace' AS source_feature_layer,
    'placeid' AS record_id_type,
    placeid::VARCHAR AS record_id,
    CASE log_message_id
        WHEN 'CSCL-LOG-SAF-26'
            THEN {{ log_message(
                "Failed to add [SAFA] record from [CommonPlace] featureclass with SEGMENTID = [{}] "
                "and PLACEID = [{}] as it is neither Geometry based nor Proto based",
                ["segmentid", "placeid"]
            ) }}
        ELSE {{ log_message(
            "Failed to add [SAFA] record from [CommonPlace] featureclass with SEGMENTID = [{}] "
            "and PLACEID = [{}] as ALT_SEGDATA_TYPE = '{}'",
            ["segmentid", "placeid", "alt_segdata_type"]
        ) }}
    END AS message
FROM failed
