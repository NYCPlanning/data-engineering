SELECT * FROM {{ ref('int_wastewater_sewershed_spine') }}
ORDER BY wastewater_facility_id
