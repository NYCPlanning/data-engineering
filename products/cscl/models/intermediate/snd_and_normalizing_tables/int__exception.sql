WITH feature_names AS (
    SELECT * FROM {{ ref('stg__facecode_and_featurename') }}
),
words AS (
    SELECT * FROM {{ ref('int__words') }}
)
SELECT
    feature_names.lookup_key,
    words.word IS NOT NULL AS matched_word,
    feature_names.place_name_sort_order AS place_name,
    -- The legacy doc requires this last-word deletion, but prod doesn't do it
    /*
    CASE
        WHEN words.word IS NOT NULL THEN REGEXP_REPLACE(feature_names.place_name_sort_order, '\s*' || words.word || '$', '')
        ELSE feature_names.place_name_sort_order
    END AS place_name,
    */
    feature_names.feature_type,
    feature_names.globalid
FROM feature_names
LEFT JOIN words ON feature_names.lookup_key LIKE '% ' || words.word --TODO recursive?
WHERE
    feature_names.exception_flag = 'Y'
    AND feature_names.enders_flag = 'N'
    -- Prod (ExceptionExtractClass) skips names shorter than 2 characters, e.g. 'E'
    AND length(trim(feature_names.place_name_sort_order)) > 1
