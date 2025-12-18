SELECT
    '1' AS record_type,
    LEFT(b10sc, 1) AS boroughcode,
    place_name_sort_order AS place_name,
    CASE
        WHEN primary_flag = 'Y' THEN 'P'
        ELSE 'V'
    END AS primary_flag,
    CASE
        WHEN principal_flag = 'Y' THEN 'F'
        ELSE 'S'
    END AS principal_flag,
    b10sc,
    CASE WHEN lookup_key ~ '% \d%' THEN 'N' END AS numeric_name_indicator,
    snd_feature_type AS geographic_feature_type,
    LENGTH(RTRIM(lookup_key)) AS name_length,
    place_name_sort_order AS full_name,
    horizontal_topo_type
FROM {{ ref('stg__facecode_and_featurename') }}
WHERE lookup_key IS NOT NULL
AND enders_flag <> 'Y'
AND b10sc IS NOT NULL
