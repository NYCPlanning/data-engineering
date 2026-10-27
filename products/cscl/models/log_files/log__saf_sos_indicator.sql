-- SAF D/F (CSCL-LOG-SAF-13, SAFExtractClass.cs:751) and O (CSCL-LOG-SAF-17, :1375)
-- AltSegmentData rows on a centerline whose SOSINDICATOR is neither left nor right ('1' or
-- '2' in our inputs; prod reads them as 'L' and 'R'). Prod still writes the record, with
-- blank house numbers, and so do int__saf_d and int__saf_o.
WITH altsegdata AS (
    SELECT * FROM {{ ref('stg__altsegmentdata_saf') }}
    WHERE saftype IN ('D', 'F', 'O')
),
centerline AS (
    SELECT DISTINCT segmentid FROM {{ source('recipe_sources', 'dcp_cscl_centerline') }}
)
SELECT
    'error' AS log_level,
    'saf side of street missing' AS error_category,
    CASE WHEN altsegdata.saftype = 'O' THEN 'CSCL-LOG-SAF-17' ELSE 'CSCL-LOG-SAF-13' END
        AS log_message_id,
    altsegdata.globalid,
    'altsegmentdata' AS source_feature_layer,
    'segmentid' AS record_id_type,
    altsegdata.segmentid AS record_id,
    CASE
        WHEN altsegdata.saftype = 'O'
            THEN {{ log_message(
                "SAFV7,SAFV9 missing in [SAFV] record from [AltSegmentData] table with "
                "SEGMENTID = [{}] and LIONKEY = [{}] as SAFV5 = {}",
                ["altsegdata.segmentid", "altsegdata.lionkey", "altsegdata.sosindicator"]
            ) }}
        ELSE {{ log_message(
            "SAFD7,SAFD8 missing in [SAFD] record from [AltSegmentData] table with "
            "SEGMENTID = [{}] and LIONKEY = [{}] as SAFD5 = {}",
            ["altsegdata.segmentid", "altsegdata.lionkey", "altsegdata.sosindicator"]
        ) }}
    END AS message
FROM altsegdata
INNER JOIN centerline ON altsegdata.segmentid = centerline.segmentid
WHERE altsegdata.sosindicator IS NULL OR altsegdata.sosindicator NOT IN ('1', '2')
