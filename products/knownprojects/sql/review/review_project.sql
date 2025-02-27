DROP TABLE IF EXISTS review_project;
WITH dcp_planner AS (
    SELECT
        project_id,
        ARRAY_TO_STRING(ARRAY_AGG(DISTINCT planner), ' ,') AS dcp_plannernames
    FROM (
        SELECT
            a.dcp_name AS planner,
            b.project_id
        FROM dcp_dcpprojectteams AS a LEFT JOIN dcp_projects AS b
            ON SPLIT_PART(a.dcp_dmsourceid, '_', 1) = b.project_id
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
    (CARDINALITY(b.project_record_ids) > 1)::integer AS multirecord_project,
    b.dummy_id,
    (a.geom IS NULL)::integer AS no_geom,
    CASE
        WHEN ST_ISEMPTY(geom) THEN NULL
        WHEN GEOMETRYTYPE(geom) = 'GEOMETRYCOLLECTION' THEN ST_MAKEVALID(ST_COLLECTIONEXTRACT(a.geom, 3))
        ELSE ST_MAKEVALID(geom)
    END AS geom,
    NOW() AS v,
    ARRAY_TO_STRING(b.project_record_ids, ',') AS project_record_ids,
    CARDINALITY(b.project_record_ids) AS records_in_project,
    ROUND(
        (ST_AREA(ST_ORIENTEDENVELOPE(a.geom)::geography))::numeric
        / (1609.34 ^ 2),
        5
    ) AS bbox_area
INTO review_project
FROM combined AS a
LEFT JOIN (
    SELECT
        project_record_ids,
        UNNEST(project_record_ids) AS record_id,
        ROW_NUMBER() OVER (
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
