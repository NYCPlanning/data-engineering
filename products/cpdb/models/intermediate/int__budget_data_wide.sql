WITH ccp_projects AS (
    SELECT * FROM {{ ref('int__ccp_projects') }}
),

-- legacy pivots fundingsource ('CNX'/'CEX'/'STA'/'FED'/'OTH') into columns via 4 separate
-- CROSSTAB()s (one per amount column), each INNER JOINed back together on maprojid. Since all 4
-- draw from the exact same (maprojid, fundingsource) row population in int__budget_data, that
-- join is equivalent to pivoting all 4 amount columns in one pass with conditional aggregation --
-- simpler, and drops the tablefunc/CROSSTAB extension dependency entirely.
pivoted AS (
    SELECT
        maprojid,
        SUM(CASE WHEN fundingsource = 'CNX' THEN adpt_am END) AS adopt_ccnonexempt,
        SUM(CASE WHEN fundingsource = 'CEX' THEN adpt_am END) AS adopt_ccexempt,
        SUM(CASE WHEN fundingsource = 'STA' THEN adpt_am END) AS adopt_nccstate,
        SUM(CASE WHEN fundingsource = 'FED' THEN adpt_am END) AS adopt_nccfederal,
        SUM(CASE WHEN fundingsource = 'OTH' THEN adpt_am END) AS adopt_nccother,
        SUM(CASE WHEN fundingsource = 'CNX' THEN ucomit_am END) AS allocate_ccnonexempt,
        SUM(CASE WHEN fundingsource = 'CEX' THEN ucomit_am END) AS allocate_ccexempt,
        SUM(CASE WHEN fundingsource = 'STA' THEN ucomit_am END) AS allocate_nccstate,
        SUM(CASE WHEN fundingsource = 'FED' THEN ucomit_am END) AS allocate_nccfederal,
        SUM(CASE WHEN fundingsource = 'OTH' THEN ucomit_am END) AS allocate_nccother,
        SUM(CASE WHEN fundingsource = 'CNX' THEN cmtmnt_am - cash_exp_am END) AS commit_ccnonexempt,
        SUM(CASE WHEN fundingsource = 'CEX' THEN cmtmnt_am - cash_exp_am END) AS commit_ccexempt,
        SUM(CASE WHEN fundingsource = 'STA' THEN cmtmnt_am - cash_exp_am END) AS commit_nccstate,
        SUM(CASE WHEN fundingsource = 'FED' THEN cmtmnt_am - cash_exp_am END) AS commit_nccfederal,
        SUM(CASE WHEN fundingsource = 'OTH' THEN cmtmnt_am - cash_exp_am END) AS commit_nccother,
        SUM(CASE WHEN fundingsource = 'CNX' THEN cash_exp_am END) AS spent_ccnonexempt,
        SUM(CASE WHEN fundingsource = 'CEX' THEN cash_exp_am END) AS spent_ccexempt,
        SUM(CASE WHEN fundingsource = 'STA' THEN cash_exp_am END) AS spent_nccstate,
        SUM(CASE WHEN fundingsource = 'FED' THEN cash_exp_am END) AS spent_nccfederal,
        SUM(CASE WHEN fundingsource = 'OTH' THEN cash_exp_am END) AS spent_nccother
    FROM {{ ref('int__budget_data') }}
    GROUP BY maprojid
)

SELECT
    ccp_projects.maprojid,
    ccp_projects.projectid,
    ccp_projects.magency,
    pivoted.adopt_ccnonexempt,
    pivoted.adopt_ccexempt,
    pivoted.adopt_ccnonexempt + pivoted.adopt_ccexempt AS adopt_citycost,
    pivoted.adopt_nccstate,
    pivoted.adopt_nccfederal,
    pivoted.adopt_nccother,
    pivoted.adopt_nccstate + pivoted.adopt_nccfederal + pivoted.adopt_nccother AS adopt_noncitycost,
    pivoted.adopt_ccnonexempt + pivoted.adopt_ccexempt + pivoted.adopt_nccstate
    + pivoted.adopt_nccfederal + pivoted.adopt_nccother AS adopt_total,
    pivoted.allocate_ccnonexempt,
    pivoted.allocate_ccexempt,
    pivoted.allocate_ccnonexempt + pivoted.allocate_ccexempt AS allocate_citycost,
    pivoted.allocate_nccstate,
    pivoted.allocate_nccfederal,
    pivoted.allocate_nccother,
    pivoted.allocate_nccstate + pivoted.allocate_nccfederal + pivoted.allocate_nccother AS allocate_noncitycost,
    pivoted.allocate_ccnonexempt + pivoted.allocate_ccexempt + pivoted.allocate_nccstate
    + pivoted.allocate_nccfederal + pivoted.allocate_nccother AS allocate_total,
    pivoted.commit_ccnonexempt,
    pivoted.commit_ccexempt,
    pivoted.commit_ccnonexempt + pivoted.commit_ccexempt AS commit_citycost,
    pivoted.commit_nccstate,
    pivoted.commit_nccfederal,
    pivoted.commit_nccother,
    pivoted.commit_nccstate + pivoted.commit_nccfederal + pivoted.commit_nccother AS commit_noncitycost,
    pivoted.commit_ccnonexempt + pivoted.commit_ccexempt + pivoted.commit_nccstate
    + pivoted.commit_nccfederal + pivoted.commit_nccother AS commit_total,
    pivoted.spent_ccnonexempt,
    pivoted.spent_ccexempt,
    pivoted.spent_ccnonexempt + pivoted.spent_ccexempt AS spent_citycost,
    pivoted.spent_nccstate,
    pivoted.spent_nccfederal,
    pivoted.spent_nccother,
    pivoted.spent_nccstate + pivoted.spent_nccfederal + pivoted.spent_nccother AS spent_noncitycost,
    pivoted.spent_ccnonexempt + pivoted.spent_ccexempt + pivoted.spent_nccstate
    + pivoted.spent_nccfederal + pivoted.spent_nccother AS spent_total
FROM ccp_projects
INNER JOIN pivoted ON ccp_projects.maprojid = pivoted.maprojid
