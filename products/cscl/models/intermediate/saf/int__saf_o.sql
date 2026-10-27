WITH saf AS (
    SELECT * FROM {{ ref('int__saf_segments') }}
),
altsegdata AS (
    SELECT * FROM {{ ref('stg__altsegmentdata_saf') }}
),
feature_names AS (
    SELECT * FROM {{ ref('stg__facecode_and_featurename_principal') }}
)
SELECT
    feature_names.place_name_sort_order AS place_name,
    saf.boroughcode,
    altsegdata.face_code,
    altsegdata.segment_seqnum,
    CASE
        WHEN altsegdata.sosindicator = '1' THEN 'L'
        WHEN altsegdata.sosindicator = '2' THEN 'R'
    END AS sos_indicator,
    altsegdata.b5sc,
    -- the house numbers of the row's side; blank when the side is neither left nor right,
    -- like prod (SAFExtractClass.cs:1375, CSCL-LOG-SAF-17)
    CASE altsegdata.sosindicator
        WHEN '1' THEN altsegdata.l_low_hn
        WHEN '2' THEN altsegdata.r_low_hn
    END AS low_hn,
    altsegdata.low_hn_suffix,
    CASE altsegdata.sosindicator
        WHEN '1' THEN altsegdata.l_high_hn
        WHEN '2' THEN altsegdata.r_high_hn
    END AS high_hn,
    altsegdata.high_hn_suffix,
    saf.saftype,
    altsegdata.lgc1,
    altsegdata.lgc2,
    altsegdata.lgc3,
    altsegdata.lgc4,
    altsegdata.boe_preferred_lgc_flag AS boe_lgc_pointer,
    saf.segment_type,
    altsegdata.segmentid,
    NULL::INT AS x_coord,
    NULL::INT AS y_coord,
    saf.saf_globalid,
    saf.saf_source_table,
    saf.generic,
    saf.roadbed
FROM saf
LEFT JOIN altsegdata ON saf.saf_globalid = altsegdata.globalid
LEFT JOIN feature_names
    ON (altsegdata.b5sc || altsegdata.lgc1) = feature_names.b7sc
WHERE saf.saftype = 'O'
