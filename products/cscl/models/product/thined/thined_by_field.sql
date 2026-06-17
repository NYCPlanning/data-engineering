SELECT
    globalid,
    {{ apply_text_formatting_from_seed('text_formatting__thined_dat') }}
FROM {{ ref('thined_by_field_unformatted') }}
ORDER BY assembly_district, election_district
