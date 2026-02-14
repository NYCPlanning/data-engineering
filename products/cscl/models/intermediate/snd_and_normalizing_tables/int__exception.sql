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
    -- Weird one - docs explicitly say this is needed but checking prod, this does not happen
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
WHERE feature_names.exception_flag = 'Y' AND feature_names.enders_flag = 'N'
