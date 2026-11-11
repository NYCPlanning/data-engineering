SELECT
    facility.*,
    functions.* EXCLUDE (wastewater_facility_id),
    performance.* EXCLUDE (wastewater_facility_id),
    performance.latest_flow_12_month_average_mgd / facility.capacity_mgd AS latest_flow_12_month_average_share_of_capacity,
    location.location_source,
    location.geom
FROM {{ ref('int_wastewater_facility_spine') }} AS facility
LEFT JOIN {{ ref('int_wastewater_facility_function_features') }} AS functions
    ON facility.wastewater_facility_id = functions.wastewater_facility_id
LEFT JOIN {{ ref('int_wastewater_facility_performance_metrics') }} AS performance
    ON facility.wastewater_facility_id = performance.wastewater_facility_id
LEFT JOIN {{ ref('int_wastewater_facility_location_features') }} AS location
    ON facility.wastewater_facility_id = location.wastewater_facility_id
ORDER BY facility.capacity_mgd DESC
