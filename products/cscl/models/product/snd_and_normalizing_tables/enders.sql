SELECT RPAD(LEFT(lookup_key, 32)) FROM {{ ref('stg__facecode_and_featurename') }}
WHERE enders_flag = 'Y'
