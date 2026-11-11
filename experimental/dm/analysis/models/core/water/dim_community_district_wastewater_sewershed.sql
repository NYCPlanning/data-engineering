SELECT * FROM {{ ref('int_cd_wastewater_sewershed_spine') }}
ORDER BY borocd, wastewater_facility_id
