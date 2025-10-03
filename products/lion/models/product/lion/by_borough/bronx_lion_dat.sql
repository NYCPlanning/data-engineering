SELECT *
FROM {{ ref('lion_dat') }}
WHERE dat_column LIKE '2%'
