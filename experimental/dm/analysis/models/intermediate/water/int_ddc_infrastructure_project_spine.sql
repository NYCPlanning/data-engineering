-- one row per DDC project, its segment lines merged. Project attributes are the same on every row of a project.
SELECT
    ddc_project_id,
    any_value(is_active) AS is_active,
    any_value(description) AS description,
    any_value(phase) AS phase,
    any_value(status) AS status,
    any_value(sponsor) AS sponsor,
    any_value(managing_agency) AS managing_agency,
    any_value(project_types) AS project_types,
    any_value(project_ca) AS project_ca,
    any_value(division) AS division,
    any_value(unit) AS unit,
    any_value(design_start) AS design_start,
    any_value(design_completion) AS design_completion,
    any_value(construction_start) AS construction_start,
    any_value(construction_completion) AS construction_completion,
    any_value(projected_) AS projected_,
    any_value(projected1) AS projected1,
    any_value(actual_sta) AS actual_sta,
    any_value(estatcompt) AS estatcompt,
    any_value(bmsecondar) AS bmsecondar,
    count(DISTINCT segment_id) AS segments,
    ST_Union_Agg(geom) AS geom
FROM {{ ref('stg_ddc__infrastructure_project_segments') }}
GROUP BY ddc_project_id
