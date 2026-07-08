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
    boroughcode || face_code || segment_seqnum AS _saf_key
FROM combined
