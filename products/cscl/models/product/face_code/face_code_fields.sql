-- Unformatted rows of face_code_by_field, for packaging (dcpy.utils.fixed_width).
SELECT
    {{ select_fields_from_seed('text_formatting__face_code') }}
FROM {{ ref("stg__facecode_and_featurename") }}
WHERE face_code IS NOT NULL
