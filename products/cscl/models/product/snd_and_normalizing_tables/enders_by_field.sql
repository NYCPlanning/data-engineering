SELECT
    RPAD(LEFT(lookup_key, 32), 32, ' ') AS lookup_key,
    RPAD(LEFT(lookup_key, 32), 32, ' ') AS _enders_key
FROM {{ ref('stg__facecode_and_featurename') }}
WHERE
    enders_flag = 'Y'
    -- prod drops StreetName rows with a NULL or empty key (CSCL-LOG-END-01)
    AND NOT (feature_type = 'street' AND COALESCE(lookup_key, '') = '')
