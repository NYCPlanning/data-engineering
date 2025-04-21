SELECT *
FROM {{ ref('lion_dat') }}
WHERE "Borough" = '1'
