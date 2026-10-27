-- Unformatted rows of enders_by_field, for packaging (dcpy.utils.fixed_width).
SELECT LEFT(lookup_key, 32) AS lookup_key
FROM {{ ref('stg__facecode_and_featurename') }}
WHERE
    enders_flag = 'Y'
    -- prod drops StreetName and FeatureName rows with a NULL or empty key
    -- (CSCL-LOG-END-01, -02)
    AND COALESCE(lookup_key, '') <> ''
