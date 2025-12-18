WITH feature_names AS (
    SELECT 
        *,
        '' AS last_word -- TODO
    FROM {{ ref('stg__facecode_and_featurename') }}
),
words AS (
    SELECT * FROM {{ ref('int__words') }}
)
SELECT
    feature_names.lookup_key,
    feature_names.last_word,
    words.word IS NOT NULL AS matched_word,
    TRIM(feature_names.lookup_key, words.word) AS place_name, --TODO this def does not work
    feature_names.feature_type,
    feature_names.globalid
FROM feature_names
LEFT JOIN words ON feature_names.last_word = words.word
WHERE exception_flag = 'Y' AND enders_flag = 'N'
