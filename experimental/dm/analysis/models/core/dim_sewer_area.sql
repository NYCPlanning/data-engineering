SELECT * FROM {{ ref('int_sewer_area_spine') }}
ORDER BY sewer_area_type
