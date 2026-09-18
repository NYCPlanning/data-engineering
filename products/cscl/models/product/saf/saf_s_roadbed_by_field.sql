WITH combined AS (
    SELECT
        {{ apply_text_formatting_from_seed('text_formatting__saf_s') }}
    FROM {{ ref("int__saf_s" ) }}
    WHERE roadbed
)
SELECT
    *,
    -- boroughcode/face_code/segmentid alone aren't unique per row - a segment can
    -- carry many SAF sub-records (different house-number ranges, sides, etc).
    -- Verified unique against a real build's output.
    boroughcode || '|' || face_code || '|' || segmentid || '|' || segment_seqnum
    || '|' || sos_indicator || '|' || hn || '|' || hn_suffix || '|'
    || high_alpha_hn_suffix || '|' || b5sc AS _saf_key
FROM combined
