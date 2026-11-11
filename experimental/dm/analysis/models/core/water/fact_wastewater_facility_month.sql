SELECT * FROM {{ ref('int_wastewater_facility_month_spine') }}
ORDER BY wastewater_facility_id, report_month
