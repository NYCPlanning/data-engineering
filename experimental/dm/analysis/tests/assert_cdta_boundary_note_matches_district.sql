-- int_cd_spine keeps only the Equivalent/Approximation part of each CDTA name's boundary note.
-- The rest repeats the district, so check that it does.
WITH notes AS (
    SELECT
        cdta2020,
        cdta_type,
        regexp_extract(cdta_name, ' \((CD|JIA) (\d+) (Equivalent|Approximation)\)$', ['area', 'number', 'match'])
            AS note
    FROM {{ ref('stg_dcp__cdta2020') }}
)

SELECT
    spine.borocd,
    notes.cdta2020,
    notes.note
FROM notes
INNER JOIN {{ ref('int_cd_spine') }} AS spine ON notes.cdta2020 = spine.cdta2020
WHERE
    notes.note.match = ''
    OR notes.note.number::integer != spine.cd_number
    OR (notes.note.area = 'JIA') != spine.is_joint_interest_area
