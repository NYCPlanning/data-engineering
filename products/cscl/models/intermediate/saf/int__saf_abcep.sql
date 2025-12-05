WITH saf AS (
    SELECT * FROM {{ ref('stg__altsegmentdata_saf') }}
),
centerline AS (
    SELECT * FROM {{ ref('stg__centerline') }}
),
feature_names AS (
    SELECT * FROM {{ ref('stg__facecode_and_featurename') }}
)
-- TODO - some of the formatting stuff here could go in staging
-- need to be sure to not interfere with how saf is used in lion
SELECT
    -- TODO "warning" issued if featurename instead of streetname
    -- TODO error when null
    -- TODO needs handling see 7.2 note 1.f
    CASE
        WHEN saf.saftype = 'C' THEN '75 STREET'
        ELSE feature_names.lookup_key
    END AS place_name,
    saf.borough AS boroughcode,
    SUBSTRING(saf.lionkey, 2, 4) AS face_code,
    SUBSTRING(saf.lionkey, 6, 5) AS segment_seqnum,
    CASE
        WHEN saf.sosindicator = '1' THEN 'L'
        WHEN saf.sosindicator = '2' THEN 'R'
    END AS sos_indicator,
    saf.b5sc,
    saf.l_low_hn,
    saf.l_high_hn,
    saf.r_low_hn,
    saf.r_high_hn,
    saf.saftype,
    saf.lgc1,
    saf.lgc2,
    saf.lgc3,
    saf.lgc4,
    saf.boe_preferred_lgc_flag AS boe_lgc_pointer,
    centerline.segment_type,
    saf.segmentid
FROM saf
LEFT JOIN centerline ON saf.segmentid = centerline.segmentid
LEFT JOIN feature_names
    ON (saf.b5sc || saf.lgc1) = feature_names.b7sc
WHERE saf.saftype IN ('A', 'B', 'C', 'E', 'P')
