-- S, P and G share one byte-identical layout; only the field labels differ
SELECT {{ apply_text_formatting_from_seed('text_formatting__ldf_segment') }}
FROM {{ ref('int__ldf_records') }}
WHERE record_type IN ('S', 'P', 'G')
