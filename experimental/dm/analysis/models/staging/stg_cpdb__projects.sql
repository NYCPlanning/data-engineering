SELECT
    maprojid,
    magencyacro AS managing_agency_acronym,
    magencyname AS managing_agency_name,
    description,
    projectid AS project_id,
    mindate AS earliest_date,
    maxdate AS latest_date,
    typecategory AS type_category,
    plannedcommit_total AS planned_commitments,
    adopt_total AS adopted,
    allocate_total AS allocated,
    commit_total AS committed,
    spent_total AS spent
FROM {{ source('sources', 'cpdb_projects') }}
