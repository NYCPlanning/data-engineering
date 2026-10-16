SELECT
    c.ccpversion AS ccp_version,
    c.maprojid,
    c.budgetline AS budget_line,
    SPLIT_PART(c.budgetline, '-', 1) AS project_type,
    c.typc,
    c.commitmentcode AS commitment_code,
    c.plancommdate AS planned_commit_date,
    c.plannedcommit_ccnonexempt,
    c.plannedcommit_ccexempt,
    c.plannedcommit_citycost,
    c.plannedcommit_nccstate,
    c.plannedcommit_nccfederal,
    c.plannedcommit_nccother,
    c.plannedcommit_noncitycost,
    c.plannedcommit_total,
    d.lifecycle
FROM {{ ref('int__ccp_commitments') }} AS c
LEFT JOIN {{ ref('dim_ten_year_plan_category') }} AS d
    ON SPLIT_PART(c.budgetline, '-', 1) = d.project_type AND c.typc = d.typc
