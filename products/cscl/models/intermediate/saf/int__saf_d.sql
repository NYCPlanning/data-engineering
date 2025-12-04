WITH saf AS (
    SELECT * FROM {{ ref('stg__altsegmentdata_saf') }}
),
centerline AS (
    SELECT * FROM {{ ref('stg__centerline') }}
),
feature_names AS (
    SELECT * FROM {{ ref('stg__facecode_and_featurename') }}
),
saf_d_regular_b5sc AS (
    SELECT *
    FROM {{ ref('saf_d_hardcoded_regular_b5sc') }}
)
-- TODO - some of the formatting stuff here could go in staging
-- need to be sure to not interfere with how saf is used in lion
SELECT
    -- TODO "warning" issued if featurename instead of streetname
    -- TODO error when null
    -- TODO needs handling see 7.2 note 1.f
    feature_names.lookup_key AS place_name,
    saf.borough AS boroughcode,
    SUBSTRING(saf.lionkey, 2, 4) AS face_code,
    SUBSTRING(saf.lionkey, 6, 5) AS segment_seqnum,
    CASE
        WHEN saf.sosindicator = '1' THEN 'L'
        WHEN saf.sosindicator = '2' THEN 'R'
    END AS sos_indicator,
    saf.b5sc AS daps_b5sc,
    COALESCE(saf.l_low_hn, saf.r_low_hn) AS low_hn,
    COALESCE(saf.l_high_hn, saf.r_high_hn) AS high_hn,
    'D' AS saftype,
    saf.zipcode,
    (saf.saftype = 'F')::INT AS daps_type,
    saf.boe_preferred_lgc_flag AS boe_lgc_pointer,
    centerline.segment_type,
    saf.segmentid,
    saf_d_regular_b5sc.regular_b5sc
FROM saf
LEFT JOIN centerline ON saf.segmentid = centerline.segmentid
LEFT JOIN feature_names
    ON (saf.b5sc || saf.lgc1) = feature_names.b7sc
-- TODO - make sure the hard-coding was up-to-date
-- TODO - per the docs, this might not need to be hardcoded
--   note that says "tool has no means", but its a very joinable thing I think
-- SEE 7.3 note 4
LEFT JOIN saf_d_regular_b5sc ON saf.b5sc::INT = saf_d_regular_b5sc.daps_b5sc
WHERE saf.saftype IN ('D', 'F')
