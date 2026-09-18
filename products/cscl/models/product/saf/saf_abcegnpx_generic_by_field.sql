WITH combined AS (
    SELECT
        {{ apply_text_formatting_from_seed('text_formatting__saf_a') }}
    FROM {{ ref("int__saf_abcep" ) }}
    WHERE generic
    UNION ALL
    SELECT
        {{ apply_text_formatting_from_seed('text_formatting__saf_a') }}
    FROM {{ ref("int__saf_gnx" ) }}
    WHERE generic
)
SELECT
    *,
    -- boroughcode/face_code/segmentid alone aren't unique per row - a segment can
    -- carry many SAF sub-records (different house-number ranges, sides, etc).
    -- side_borough_code/side_ct2020_basic/side_ct2020_suffix/side_ap are
    -- deliberately excluded - they're computed side-of-street geo joins that can
    -- legitimately differ from prod (e.g. an atomic-polygon boundary call), so
    -- putting them in the key would hide real diffs instead of catching them.
    -- Verified unique (bar one known place_name-spelling-variant pair) against a
    -- real build's output.
    boroughcode || '|' || face_code || '|' || segmentid || '|' || segment_seqnum
    || '|' || sos_indicator || '|' || b5sc || '|' || l_low_hn || '|' || l_high_hn
    || '|' || r_low_hn || '|' || r_high_hn || '|' || x_coord || '|' || y_coord
        AS _saf_key
FROM combined
