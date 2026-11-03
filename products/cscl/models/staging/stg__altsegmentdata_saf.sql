SELECT
    lionkey,
    borough,
    SUBSTRING(lionkey, 2, 4) AS face_code,
    SUBSTRING(lionkey, 6, 5) AS segment_seqnum,
    segmentid,
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
    zipcode,
    boe_preferred_lgc_flag,
    b5sc,
    sosindicator,
    alt_segdata_type,
    NULLIF(TRIM(alt_segdata_type), '') IS NULL AS is_blank_alt_segdata_type,
    lgc5,
    lgc6,
    lgc7,
    lgc8,
    lgc9,
    globalid,
    ogc_fid
FROM {{ source("recipe_sources", "dcp_cscl_altsegmentdata") }}
-- CSCL-SAF-03: spec Table 13 makes only ALT_SEGDATA_TYPE S rows SAF records, but prod
-- selects them by SAFTYPE alone (SAFExtractClass.cs:1448, SAFTYPE IN ('A', 'B', 'C', 'E',
-- 'P'); CSCLDataHelpersClass.cs:1215-1300, GetSAFTData, for the LION gdb), so it reads a
-- row with a blank ALT_SEGDATA_TYPE and a SAF type as one too. Kept unless the var
-- fix_blank_type_altsegmentdata is on.
WHERE
    alt_segdata_type = 'S'
    {%- if not var('fix_blank_type_altsegmentdata') %}
        OR (NULLIF(TRIM(alt_segdata_type), '') IS NULL AND NULLIF(TRIM(saftype), '') IS NOT NULL)
    {%- endif %}
