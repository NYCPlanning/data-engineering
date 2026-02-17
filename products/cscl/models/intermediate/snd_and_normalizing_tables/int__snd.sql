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
    CASE WHEN lookup_key ~ '(^| )[[:digit:]]' THEN 'N' END AS numeric_name_indicator,
    snd_feature_type AS geographic_feature_type,
    LENGTH(RTRIM(place_name_sort_order)) AS name_length,
    lookup_key_trimmed AS full_name,
    --TRIM(REGEXP_REPLACE(place_name_sort_order, '\s+', ' ', 'g')) AS full_name, -- this is wrong when lookup key has letter/dash followed by digit
    horizontal_topo_type
FROM {{ ref('stg__facecode_and_featurename') }}
WHERE
    lookup_key IS NOT NULL
    AND enders_flag IS DISTINCT FROM 'Y'
    AND b10sc IS NOT NULL
