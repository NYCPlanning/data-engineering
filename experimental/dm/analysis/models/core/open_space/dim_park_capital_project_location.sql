SELECT * FROM {{ ref('int_park_capital_project_location_spine') }}
ORDER BY tracker_id, location_number
