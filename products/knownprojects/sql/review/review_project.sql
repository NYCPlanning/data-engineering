DROP TABLE IF EXISTS review_project;
WITH dcp_planner AS (
    SELECT
        project_id,
        array_to_string(array_agg(DISTINCT planner), ' ,') AS dcp_plannernames
    FROM (
        SELECT
            a.dcp_name AS planner,
            b.project_id
        FROM dcp_dcpprojectteams AS a LEFT JOIN dcp_projects AS b
            ON split_part(a.dcp_dmsourceid, '_', 1) = b.project_id
    ) AS a
    GROUP BY project_id
)

SELECT
    a.source,
    a.record_id,
    a.record_name,
    dcp_projects.project_brief,
    dcp_planner.dcp_plannernames,
    -- dcp_projects.dcp_communitydistricts,
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
    (cardinality(b.project_record_ids) > 1)::integer AS multirecord_project,
    b.dummy_id,
    (a.geom IS NULL)::integer AS no_geom,
    CASE
        WHEN ST_IsEmpty(geom) THEN NULL
        WHEN geometrytype(geom) = 'GEOMETRYCOLLECTION' THEN ST_MakeValid(ST_CollectionExtract(a.geom, 3))
        ELSE ST_MakeValid(geom)
    END AS geom,
    now() AS v,
    array_to_string(b.project_record_ids, ',') AS project_record_ids,
    cardinality(b.project_record_ids) AS records_in_project,
    round(
        (ST_Area(ST_OrientedEnvelope(a.geom)::geography))::numeric
        / (1609.34 ^ 2),
        5
    ) AS bbox_area
INTO review_project
FROM combined AS a
LEFT JOIN (
    SELECT
        project_record_ids,
        unnest(project_record_ids) AS record_id,
        row_number() OVER (
            ORDER BY project_record_ids
        ) AS dummy_id
    FROM _project_record_ids
) AS b
    ON a.record_id = b.record_id
LEFT JOIN dcp_planner ON a.record_id = dcp_planner.project_id
LEFT JOIN dcp_projects ON a.record_id = dcp_projects.project_id
WHERE
    a.source NOT IN (
        'DOB',
        'Neighborhood Study Rezoning Commitments',
        'Future Neighborhood Studies'
    );
