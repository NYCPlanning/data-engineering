WITH combined AS (
    SELECT
        {{ apply_text_formatting_from_seed('text_formatting__saf_v') }}
    FROM {{ ref("int__saf_o" ) }}
    WHERE generic
    UNION ALL
    SELECT
        {{ apply_text_formatting_from_seed('text_formatting__saf_v') }}
    FROM {{ ref("int__saf_v" ) }}
    WHERE generic
)
SELECT
    *,
    -- boroughcode/face_code/segmentid alone aren't unique per row - a segment can
    -- carry many SAF sub-records (different house-number ranges, sides, etc).
    -- Verified unique against a real build's output.
    boroughcode || '|' || face_code || '|' || segmentid || '|' || segment_seqnum
    || '|' || sos_indicator || '|' || b5sc || '|' || low_hn || '|' || low_hn_suffix
    || '|' || high_hn || '|' || high_hn_suffix || '|' || x_coord || '|' || y_coord
    AS _saf_key
FROM combined
