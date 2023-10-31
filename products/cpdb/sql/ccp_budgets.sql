DROP TABLE IF EXISTS ccp_budgets;
CREATE TABLE ccp_budgets AS
SELECT
    ccpversion,
    magency,
    projectid,
    maprojid,
    budgetline,
    projecttype,
    sagencyacro,
    sagencyname,
    sum(plannedcommit_ccnonexempt) AS plannedcommit_ccnonexempt,
    sum(plannedcommit_ccexempt) AS plannedcommit_ccexempt,
    sum(plannedcommit_citycost) AS plannedcommit_citycost,
    sum(plannedcommit_nccstate) AS plannedcommit_nccstate,
    sum(plannedcommit_nccfederal) AS plannedcommit_nccfederal,
    sum(plannedcommit_noncitycost) AS plannedcommit_noncitycost,
    sum(plannedcommit_total) AS plannedcommit_total
FROM ccp_commitments
GROUP BY
    ccpversion,
    magency,
    projectid,
    maprojid,
    budgetline,
    projecttype,
    sagencyacro,
    sagencyname;
