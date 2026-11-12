-- Flow from the latest monthly report.
SELECT
    wastewater_facility_id,
    report_month AS latest_report_month,
    flow_mgd AS latest_flow_mgd,
    flow_12_month_average_mgd AS latest_flow_12_month_average_mgd,
    flow_permit_mgd AS latest_flow_permit_mgd
FROM {{ ref('int_wastewater_facility_month_spine') }}
QUALIFY report_month = max(report_month) OVER (PARTITION BY wastewater_facility_id)
