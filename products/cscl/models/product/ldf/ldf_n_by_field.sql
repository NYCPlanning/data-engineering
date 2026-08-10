SELECT {{ apply_text_formatting_from_seed('text_formatting__ldf_n') }}
FROM {{ ref('int__ldf_records') }}
WHERE record_type = 'N'
