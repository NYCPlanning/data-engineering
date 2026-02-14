SELECT
    {{ apply_text_formatting_from_seed('text_formatting__face_code') }}
FROM {{ ref("stg__facecode_and_featurename") }}
WHERE face_code IS NOT NULL
