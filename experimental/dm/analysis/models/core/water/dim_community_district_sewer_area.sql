SELECT * FROM {{ ref('int_cd_sewer_area_spine') }}
ORDER BY borocd, sewer_area_type
