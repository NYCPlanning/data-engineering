WITH combined AS (
    SELECT
        {{ apply_text_formatting_from_seed('text_formatting__saf_i') }}
    FROM {{ ref("int__saf_i" ) }}
)
SELECT
    *,
    nodeid::text AS _saf_key
FROM combined
