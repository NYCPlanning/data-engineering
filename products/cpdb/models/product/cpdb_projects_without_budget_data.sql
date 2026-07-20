SELECT
    ccpversion,
    maprojid,
    magencyacro,
    magency,
    magencyname,
    description,
    projectid,
    mindate,
    maxdate,
    typecategory,
    plannedcommit_ccnonexempt,
    plannedcommit_ccexempt,
    plannedcommit_citycost,
    plannedcommit_nccstate,
    plannedcommit_nccfederal,
    plannedcommit_nccother,
    plannedcommit_noncitycost,
    plannedcommit_total
FROM {{ ref('cpdb_projects') }}
WHERE adopt_total IS NULL
