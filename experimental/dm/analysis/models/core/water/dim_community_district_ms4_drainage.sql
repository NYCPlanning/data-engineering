SELECT * FROM {{ ref('int_cd_ms4_drainage_spine') }}
ORDER BY borocd, drains_to_type
