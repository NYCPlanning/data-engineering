-- SAF S address points prod leaves out of SAF (int__saf_s_checks, which int__saf_s
-- drops). Prod writes the AddressPoint OBJECTID into each message; we write the
-- ADDRESSPOINTID.
WITH failed AS (
    SELECT * FROM {{ ref('int__saf_s_checks') }}
)
SELECT
    CASE log_message_id WHEN 'CSCL-LOG-SAF-11' THEN 'info' ELSE 'error' END AS log_level,
    'saf s record not written' AS error_category,
    log_message_id,
    globalid,
    'addresspoints' AS source_feature_layer,
    'addresspointid' AS record_id_type,
    addresspointid::VARCHAR AS record_id,
    CASE log_message_id
        WHEN 'CSCL-LOG-SAF-06'
            THEN {{ log_message(
                "[SAF S: NO FACE CODE]  No FaceCode found using the query 'B7SC = '{}' AND "
                "PRINCIPAL_FLAG = 'Y'' in the StreetName table.  The AddressPoint feature with "
                "an AddressPointID = {} will not be written to the output file.",
                ["b7sc_actual", "addresspointid"]
            ) }}
        WHEN 'CSCL-LOG-SAF-07'
            THEN {{ log_message(
                "[SAF S: MISSING PROTO SEGMENT] The AltSegmentData does not contain a matching "
                "record using the query 'SegmentID = {}'.  The AddressPoint feature with an "
                "AddressPointID = {} will not be written to the output file.",
                ["segmentid", "addresspointid"]
            ) }}
        WHEN 'CSCL-LOG-SAF-08'
            THEN {{ log_message(
                "[SAF S: INCORRECT ALT_SEGDATA_TYPE] The matching proto segment does not have "
                "the correct Alt_SegData_Type, expected 'B' but found '{}.  The AddressPoint "
                "feature with an AddressPointID = {} will not be written to the output file.",
                ["UPPER(proto_type)", "addresspointid"]
            ) }}
        WHEN 'CSCL-LOG-SAF-09'
            THEN {{ log_message(
                "[SAF S: INCORRECT BOROUGH] The matching proto segment does not have the "
                "correct Borough, expected '{}' but found '{}.  The AddressPoint feature with "
                "an AddressPointID = {} will not be written to the output file.",
                ["proto_borough", "boroughcode", "addresspointid"]
            ) }}
        WHEN 'CSCL-LOG-SAF-10'
            THEN {{ log_message(
                "[SAF S: INCORRECT B7SC] The matching proto segment does not have the correct "
                "B7SC, expected '{}' but found '{}.  The AddressPoint feature with an "
                "AddressPointID = {} will not be written to the output file.",
                ["proto_b7sc", "TRIM(b7sc_actual)", "addresspointid"]
            ) }}
        ELSE {{ log_message(
            "Neither the house number or suffix values contain the section letter, unable to "
            "convert to the LION address format.  The SAF row with an AddressPointID = {} will "
            "not be written to the output file.",
            ["addresspointid"]
        ) }}
    END AS message
FROM failed
