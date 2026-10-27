WITH saf AS (
    SELECT * FROM {{ ref('int__saf_segments') }}
),
altsegdata AS (
    SELECT * FROM {{ ref('stg__altsegmentdata_saf') }}
),
feature_names AS (
    SELECT * FROM {{ ref('stg__facecode_and_featurename_principal') }}
),
saf_d_regular_b5sc AS (
    SELECT *
    FROM {{ ref('saf_d_hardcoded_regular_b5sc') }}
)
SELECT
    -- TODO "warning" issued if featurename instead of streetname
    -- TODO error when null
    feature_names.place_name_sort_order AS place_name,
    saf.boroughcode,
    altsegdata.face_code,
    altsegdata.segment_seqnum,
    CASE
        WHEN altsegdata.sosindicator = '1' THEN 'L'
        WHEN altsegdata.sosindicator = '2' THEN 'R'
    END AS sos_indicator,
    altsegdata.b5sc AS daps_b5sc,
    -- the house numbers of the row's side; blank when the side is neither left nor right,
    -- like prod (SAFExtractClass.cs:751, CSCL-LOG-SAF-13)
    CASE altsegdata.sosindicator
        WHEN '1' THEN altsegdata.l_low_hn
        WHEN '2' THEN altsegdata.r_low_hn
    END AS low_hn,
    CASE altsegdata.sosindicator
        WHEN '1' THEN altsegdata.l_high_hn
        WHEN '2' THEN altsegdata.r_high_hn
    END AS high_hn,
    'D' AS saftype,
    altsegdata.zipcode,
    CASE WHEN altsegdata.saftype = 'F' THEN 1 END AS daps_type,
    altsegdata.boe_preferred_lgc_flag AS boe_lgc_pointer,
    saf.segment_type,
    saf.segmentid,
    saf_d_regular_b5sc.regular_b5sc,
    saf.saf_globalid,
    saf.saf_source_table,
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
WHERE
    saf.saftype IN ('D', 'F')
    -- prod leaves out a row whose DAPS street code isn't one of its hard-coded ones, and
    -- logs it (CSCL-LOG-SAF-14, log__saf_d_daps_b5sc)
    AND saf_d_regular_b5sc.daps_b5sc IS NOT NULL
