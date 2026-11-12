SELECT * FROM {{ ref('int_ms4_outfall_spine') }}
ORDER BY outfall_id
