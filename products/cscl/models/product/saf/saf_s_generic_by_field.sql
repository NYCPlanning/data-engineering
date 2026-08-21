WITH combined AS (
    SELECT
        {{ apply_text_formatting_from_seed('text_formatting__saf_s') }}
    FROM {{ ref("int__saf_s" ) }}
    WHERE generic
)
SELECT
    *,
    boroughcode || face_code || segmentid AS _saf_key
FROM combined
