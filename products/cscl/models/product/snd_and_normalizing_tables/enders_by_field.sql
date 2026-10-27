SELECT
    RPAD(LEFT(lookup_key, 32), 32, ' ') AS lookup_key,
    RPAD(LEFT(lookup_key, 32), 32, ' ') AS _enders_key,
    globalid AS _globalid
FROM {{ ref('stg__facecode_and_featurename') }}
WHERE
    enders_flag = 'Y'
    -- prod drops StreetName and FeatureName rows with a NULL or empty key
    -- (CSCL-LOG-END-01, -02)
    AND COALESCE(lookup_key, '') <> ''
