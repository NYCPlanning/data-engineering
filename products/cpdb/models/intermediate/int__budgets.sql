SELECT
    ccpversion,
    magency,
    projectid,
    maprojid,
    budgetline,
    projecttype,
    sagencyacro,
    sagencyname,
    SUM(plannedcommit_ccnonexempt) AS plannedcommit_ccnonexempt,
    SUM(plannedcommit_ccexempt) AS plannedcommit_ccexempt,
    SUM(plannedcommit_citycost) AS plannedcommit_citycost,
    SUM(plannedcommit_nccstate) AS plannedcommit_nccstate,
    SUM(plannedcommit_nccfederal) AS plannedcommit_nccfederal,
    SUM(plannedcommit_noncitycost) AS plannedcommit_noncitycost,
    SUM(plannedcommit_total) AS plannedcommit_total
FROM {{ ref('int__ccp_commitments') }}
GROUP BY
    ccpversion,
    magency,
    projectid,
    maprojid,
    budgetline,
    projecttype,
    sagencyacro,
    sagencyname
