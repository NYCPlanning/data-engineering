-- each project's Capital Projects Dashboard agency projects (pid) and their schedules. One
-- agency project can be funded by several FMS IDs, so a pid can show up under several projects
SELECT
    m.maprojid,
    d.pid,
    d.fms_id,
    d.managing_agency,
    d.sponsor_agency,
    d.agency_project_name,
    d.agency_project_description,
    d.current_phase,
    d.current_phase_start,
    d.forecast_current_phase_end,
    d.forecast_completion,
    d.actual_design_start,
    d.actual_design_end,
    d.actual_construction_procurement_start,
    d.actual_construction_procurement_end,
    d.actual_construction_start,
    d.actual_construction_end,
    d.borough,
    d.community_board,
    d.reporting_period,
    d.agency_data_date,
    d.fms_data_date
FROM {{ ref('int__project_dashboard_matches') }} AS m
INNER JOIN {{ ref('int__dashboard_agency_projects') }} AS d
    ON m.fms_id = d.fms_id AND m.managing_agency = d.managing_agency
