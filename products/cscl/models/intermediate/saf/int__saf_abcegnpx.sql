WITH unioned AS (
    SELECT * FROM {{ ref("int__saf_abcep" )}}
    UNION ALL
    SELECT * FROM {{ ref("int__saf_gnx" )}}
)
SELECT
    place_name,
    boroughcode,
    face_code,
    segment_seqnum,
    sos_indicator,
    b5sc,
    l_low_hn,
    l_high_hn,
    r_low_hn,
    r_high_hn,
    saftype,
    lgc1,
    lgc2,
    lgc3,
    lgc4,
    boe_lgc_pointer,
    segment_type,
    segmentid,
    segment_type IN ('B', 'E', 'U') OR (segment_type = 'G' AND incex_flag <> 'E') OR segment_type IS NULL AS generic,
    segment_type IN ('E', 'R', 'S', 'U') OR segment_type IS NULL AS roadbed
FROM unioned
WHERE 
