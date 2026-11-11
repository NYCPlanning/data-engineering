SELECT * FROM {{ ref('int_cd_stormwater_flooding_spine') }}
ORDER BY borocd, flooding_category
