SELECT
    *,
    LEFT(lionkey, 1) AS boroughcode,
    SUBSTRING(lionkey, 2, 4) AS face_code,
    SUBSTRING(lionkey, 6, 5) AS segment_seqnum,
    RIGHT(b5sc, 5) AS five_digit_street_code,
    'dcp_cscl_altsegmentdata' AS source_table
FROM {{ source("recipe_sources", "dcp_cscl_altsegmentdata") }}
WHERE
    alt_segdata_type != 'S' -- S denotes an SAF record
    AND segmentid IS NOT NULL
