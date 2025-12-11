WITH saf AS (
    SELECT * FROM {{ ref("int__saf_abcep" ) }}
    UNION ALL
    SELECT * FROM {{ ref("int__saf_gnx" ) }}
)
SELECT
    {{ apply_text_formatting_from_seed('text_formatting__saf_a') }}
FROM saf
WHERE roadbed
