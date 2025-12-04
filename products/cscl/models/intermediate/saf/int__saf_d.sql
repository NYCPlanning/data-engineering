WITH saf AS (
    SELECT * FROM {{ ref('int__saf_segments') }}
),
altsegdata AS (
    SELECT * FROM {{ ref('stg__altsegmentdata_saf') }}
),
feature_names AS (
    SELECT * FROM {{ ref('stg__facecode_and_featurename') }}
),
saf_d_regular_b5sc AS (
    SELECT *
    FROM {{ ref('saf_d_hardcoded_regular_b5sc') }}
)
SELECT
    -- TODO "warning" issued if featurename instead of streetname
    -- TODO error when null
    feature_names.saf_place_name AS place_name,
    saf.boroughcode,
    altsegdata.face_code,
    altsegdata.segment_seqnum,
    CASE
        WHEN altsegdata.sosindicator = '1' THEN 'L'
        WHEN altsegdata.sosindicator = '2' THEN 'R'
    END AS sos_indicator,
    altsegdata.b5sc AS daps_b5sc,
    COALESCE(NULLIF(altsegdata.l_low_hn, '0'), altsegdata.r_low_hn) AS low_hn,
    COALESCE(NULLIF(altsegdata.l_high_hn, '0'), altsegdata.r_high_hn) AS high_hn,
    'D' AS saftype,
    altsegdata.zipcode,
    CASE WHEN altsegdata.saftype = 'F' THEN 1 END AS daps_type,
    altsegdata.boe_preferred_lgc_flag AS boe_lgc_pointer,
    saf.segment_type,
    saf.segmentid,
    saf_d_regular_b5sc.regular_b5sc,
    saf.generic,
    saf.roadbed
FROM saf
LEFT JOIN altsegdata ON saf.saf_globalid = altsegdata.globalid
LEFT JOIN feature_names
    ON (altsegdata.b5sc || altsegdata.lgc1) = feature_names.b7sc
-- TODO - per the docs, this might not need to be hardcoded
--   note that says "tool has no means", but its a very joinable thing I think?
--   SEE 7.3 note 4
LEFT JOIN saf_d_regular_b5sc ON altsegdata.b5sc::INT = saf_d_regular_b5sc.daps_b5sc
WHERE saf.saftype IN ('D', 'F')
