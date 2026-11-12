-- one row per district, project, and budget line type. One row has no project.
SELECT
    borocd,
    maprojid,
    typc,
    budget_proj_type,
    description,
    amt AS planned_commitments_in_district,
    typecategory AS type_category,
    geomsource AS geometry_source
FROM {{ source('sources', 'cpdb_projects_by_communitydist') }}
WHERE maprojid IS NOT NULL
