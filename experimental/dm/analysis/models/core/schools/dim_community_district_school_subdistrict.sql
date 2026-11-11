SELECT * FROM {{ ref('int_cd_school_subdistrict_spine') }}
ORDER BY borocd, school_subdistrict_id
