DROP VIEW IF EXISTS fisa_budget_data_init;
CREATE VIEW fisa_budget_data_init AS

SELECT
    lpad(mng_dpt_cd, 3, '0') || cptl_proj_id AS maprojid,
    lpad(mng_dpt_cd, 3, '0') AS magency,
    cptl_proj_id AS projectid,
    rcls_cd AS occurance,
    atyp_cd AS fundingsource,
    bud_obj_cd AS commitmentcode,
    au_cd AS appropriationunit,
    cmtmnt_am::numeric,
    oblgtns_am::numeric,
    adpt_am::numeric,
    penc_am::numeric,
    enc_am::numeric,
    acrd_exp_am::numeric,
    cash_exp_am::numeric,
    ucomit_am::numeric,
    actu_exp_am::numeric
FROM fisa_dailybudget;

DROP TABLE IF EXISTS cpdb_budget_data;
CREATE TABLE cpdb_budget_data AS

SELECT
    b.maprojid,
    b.magency,
    b.projectid,
    b.fundingsource,
    sum(cmtmnt_am) AS cmtmnt_am,
    sum(oblgtns_am) AS oblgtns_am,
    sum(adpt_am) AS adpt_am,
    sum(penc_am) AS penc_am,
    sum(enc_am) AS enc_am,
    sum(acrd_exp_am) AS acrd_exp_am,
    sum(cash_exp_am) AS cash_exp_am,
    sum(ucomit_am) AS ucomit_am,
    sum(actu_exp_am) AS actu_exp_am
FROM ccp_projects AS a
INNER JOIN fisa_budget_data_init AS b ON a.maprojid = b.maprojid
GROUP BY b.magency, b.projectid, b.maprojid, b.fundingsource;


DROP TABLE IF EXISTS cpdb_budget_data_wide;
CREATE TABLE cpdb_budget_data_wide AS
SELECT
    projects.maprojid,
    projects.projectid,
    projects.magency,
    adopt.adopt_ccnonexempt,
    adopt.adopt_ccexempt,
    adopt.adopt_ccnonexempt + adopt.adopt_ccexempt AS adopt_citycost,
    adopt.adopt_nccstate,
    adopt.adopt_nccfederal,
    adopt.adopt_nccother,
    adopt.adopt_nccstate + adopt.adopt_nccfederal + adopt.adopt_nccother AS adopt_noncitycost,
    adopt.adopt_ccnonexempt
    + adopt.adopt_ccexempt
    + adopt.adopt_nccstate
    + adopt.adopt_nccfederal
    + adopt.adopt_nccother AS adopt_total,
    allocate.allocate_ccnonexempt,
    allocate.allocate_ccexempt,
    allocate.allocate_ccnonexempt + allocate.allocate_ccexempt AS allocate_citycost,
    allocate.allocate_nccstate,
    allocate.allocate_nccfederal,
    allocate.allocate_nccother,
    allocate.allocate_nccstate + allocate.allocate_nccfederal + allocate.allocate_nccother AS allocate_noncitycost,
    allocate.allocate_ccnonexempt
    + allocate.allocate_ccexempt
    + allocate.allocate_nccstate
    + allocate.allocate_nccfederal
    + allocate.allocate_nccother AS allocate_total,
    commit.commit_ccnonexempt,
    commit.commit_ccexempt,
    commit.commit_ccnonexempt + commit.commit_ccexempt AS commit_citycost,
    commit.commit_nccstate,
    commit.commit_nccfederal,
    commit.commit_nccother,
    commit.commit_nccstate + commit.commit_nccfederal + commit.commit_nccother AS commit_noncitycost,
    commit.commit_ccnonexempt
    + commit.commit_ccexempt
    + commit.commit_nccstate
    + commit.commit_nccfederal
    + commit.commit_nccother AS commit_total,
    spent.spent_ccnonexempt,
    spent.spent_ccexempt,
    spent.spent_ccnonexempt + spent.spent_ccexempt AS spent_citycost,
    spent.spent_nccstate,
    spent.spent_nccfederal,
    spent.spent_nccother,
    spent.spent_nccstate + spent.spent_nccfederal + spent.spent_nccother AS spent_noncitycost,
    spent.spent_ccnonexempt
    + spent.spent_ccexempt
    + spent.spent_nccstate
    + spent.spent_nccfederal
    + spent.spent_nccother AS spent_total
FROM ccp_projects AS projects
INNER JOIN crosstab(
    'SELECT 
        maprojid, 
        fundingsource, 
        adpt_am 
    FROM cpdb_budget_data
    ORDER BY 1, 2'
) AS adopt (
    maprojid text,
    adopt_ccexempt numeric,
    adopt_ccnonexempt numeric,
    adopt_nccfederal numeric,
    adopt_nccother numeric,
    adopt_nccstate numeric
) ON projects.maprojid = adopt.maprojid
INNER JOIN crosstab(
    'SELECT 
        maprojid, 
        fundingsource, 
        ucomit_am 
    FROM cpdb_budget_data
    ORDER BY 1, 2'
) AS allocate (
    maprojid text,
    allocate_ccexempt numeric,
    allocate_ccnonexempt numeric,
    allocate_nccfederal numeric,
    allocate_nccother numeric,
    allocate_nccstate numeric
) ON projects.maprojid = allocate.maprojid
INNER JOIN crosstab(
    'SELECT 
        maprojid, 
        fundingsource, 
        cmtmnt_am - cash_exp_am
    FROM cpdb_budget_data
    ORDER BY 1, 2'
) AS commit (
    maprojid text,
    commit_ccexempt numeric,
    commit_ccnonexempt numeric,
    commit_nccfederal numeric,
    commit_nccother numeric,
    commit_nccstate numeric
) ON projects.maprojid = commit.maprojid
INNER JOIN crosstab(
    'SELECT 
        maprojid, 
        fundingsource, 
        cash_exp_am 
    FROM cpdb_budget_data
    ORDER BY 1, 2'
) AS spent (
    maprojid text,
    spent_ccexempt numeric,
    spent_ccnonexempt numeric,
    spent_nccfederal numeric,
    spent_nccother numeric,
    spent_nccstate numeric
) ON projects.maprojid = spent.maprojid;
