WITH combined AS (
    SELECT
        {{ apply_text_formatting_from_seed('text_formatting__saf_d') }}
    FROM {{ ref("int__saf_d" ) }}
    WHERE generic
)
SELECT
    *,
    -- boroughcode/face_code/segmentid alone aren't unique per row - a segment can
    -- carry many SAF sub-records (different house-number ranges, daps types, etc).
    -- Verified unique against a real build's output.
    boroughcode || '|' || face_code || '|' || segmentid || '|' || segment_seqnum
    || '|' || sos_indicator || '|' || daps_b5sc || '|' || low_hn || '|' || high_hn
    || '|' || regular_b5sc || '|' || zipcode || '|' || daps_type AS _saf_key
FROM combined
