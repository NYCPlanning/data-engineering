SELECT
    'dcp_cscl_altsegmentdata' AS source_table,
    segmentid,
    lionkey,
    LEFT(lionkey, 1) AS boroughcode,
    SUBSTRING(lionkey, 2, 4) AS face_code,
    SUBSTRING(lionkey, 6, 5) AS segment_seqnum,
    RIGHT(b5sc, 5) AS five_digit_street_code,
    saftype,
    l_low_hn,
    l_high_hn,
    r_low_hn,
    r_high_hn,
    low_hn_suffix,
    high_hn_suffix,
    lgc1,
    lgc2,
    lgc3,
    lgc4,
    borough,
    zipcode,
    created_by,
    created_date,
    modified_by,
    modified_date,
    alt_segment_seqnum,
    boe_preferred_lgc_flag AS boe_lgc_pointer,
    b5sc,
    sosindicator,
    feature_type AS feature_type_code,
    CASE
        WHEN feature_type IS NULL OR feature_type IN ('5', '6', '9', 'A', 'W') THEN 'centerline'
        WHEN feature_type IN ('3', '4', '7', '8') THEN 'nonstreetfeatures'
        WHEN feature_type = '1' THEN 'rail'
        ELSE feature_type
    END AS feature_type,
    from_to_indicator,
    alt_segdata_type,
    seglocstatus,
    lgc5,
    lgc6,
    lgc7,
    lgc8,
    lgc9,
    twisted_parity_flag,
    globalid,
    lsubsect,
    rsubsect,
    sandist_ind,
    ogc_fid,
    data_library_version
FROM {{ source("recipe_sources", "dcp_cscl_altsegmentdata") }}
WHERE
    alt_segdata_type != 'S' -- S denotes an SAF record
    AND segmentid IS NOT NULL
