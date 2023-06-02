DROP TABLE IF EXISTS review_project;
SELECT
    NOW() as v,
    a.source,
    a.record_id,
    a.record_name,
    dcp_projects.dcp_projectbrief,
    dcp_planner.dcp_plannernames,
    dcp_projects.dcp_communitydistricts,
    a.status,
    a.type,
    a.units_gross,
    a.date,
    a.date_type,
    a.prop_within_5_years,
    a.prop_5_to_10_years,
    a.prop_after_10_years,
    a.phasing_known,
    a.nycha,
    a.classb,
    a.senior_housing,
    array_to_string(b.project_record_ids, ',') as project_record_ids,
    cardinality(b.project_record_ids) as records_in_project,
    (cardinality(b.project_record_ids) > 1)::integer as multirecord_project,
    b.dummy_id,
    ROUND((ST_Area(ST_OrientedEnvelope(a.geom)::geography))::numeric/(1609.34^2), 5) as bbox_area,
    (a.geom IS NULL)::integer as no_geom,
    a.geom
INTO review_project
FROM combined a
LEFT JOIN (
        SELECT 
            project_record_ids,
            unnest(project_record_ids) as record_id, 
            ROW_NUMBER() OVER(ORDER BY project_record_ids) as dummy_id
        FROM _project_record_ids
    ) b 
ON a.record_id = b.record_id
LEFT JOIN (
    SELECT dcp_name, array_to_string(array_agg(distinct(planner)), ' ,') as dcp_plannernames
    from (
        SELECT a.dcp_name AS planner, b.dcp_name
        FROM dcp_dcpprojectteams a LEFT JOIN dcp_projects b
        ON split_part(a.dcp_dmsourceid, '_', 1) = b.dcp_name
    ) a group by dcp_name
) dcp_planner ON a.record_id = dcp_planner.dcp_name
LEFT JOIN dcp_projects ON a.record_id = dcp_projects.dcp_name
WHERE a.source NOT IN ('DOB', 'Neighborhood Study Rezoning Commitments', 'Future Neighborhood Studies');
