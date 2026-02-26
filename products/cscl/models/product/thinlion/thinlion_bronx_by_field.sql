SELECT *
FROM {{ ref('thinlion_by_field') }}
WHERE borough = '2'
