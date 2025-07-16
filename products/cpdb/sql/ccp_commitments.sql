UPDATE fisa_capitalcommitments
SET
    fcst_cnx_amt = (CASE WHEN fcst_cnx_amt ~* '_' THEN '0' ELSE fcst_cnx_amt END),
    fcst_st_amt = (CASE WHEN fcst_st_amt ~* '_' THEN '0' ELSE fcst_st_amt END),
    fcst_cex_amt = (CASE WHEN fcst_cex_amt ~* '_' THEN '0' ELSE fcst_cex_amt END),
    fcst_fd_amt = (CASE WHEN fcst_fd_amt ~* '_' THEN '0' ELSE fcst_fd_amt END),
    fcst_pv_amt = (CASE WHEN fcst_pv_amt ~* '_' THEN '0' ELSE fcst_pv_amt END);

DROP TABLE IF EXISTS ccp_commitments CASCADE;
CREATE TABLE ccp_commitments AS
SELECT
    'fisa_' || p.cycle_fy AS ccpversion,
    lpad(p.managing_agcy_cd, 3, '0') AS magency,
    replace(p.project_id, ' ', '') AS projectid,
    lpad(p.managing_agcy_cd, 3, '0') || replace(p.project_id, ' ', '') AS maprojid,
    replace(p.budget_proj_type, ' ', '') || '-' || p.budget_line_id AS budgetline,
    b.projecttype,
    b.agencyacronym AS sagencyacro,
    b.agency AS sagencyname,
    right(p.planned_commit_date, 2) || '/' || substring(p.planned_commit_date FROM 3 FOR 2) AS plancommdate,
    p.short_descr AS projectdescription,
    p.object_name AS commitmentdescription,
    p.object AS commitmentcode,
    p.typ_category AS typc,
    p.typ_category_name AS typcname,
    p.fcst_cnx_amt::double precision AS plannedcommit_ccnonexempt,
    p.fcst_cex_amt::double precision AS plannedcommit_ccexempt,
    p.fcst_cnx_amt::double precision + p.fcst_cex_amt::double precision AS plannedcommit_citycost,
    p.fcst_st_amt::double precision AS plannedcommit_nccstate,
    p.fcst_fd_amt::double precision AS plannedcommit_nccfederal,
    p.fcst_pv_amt::double precision AS plannedcommit_nccother,
    p.fcst_st_amt::double precision
    + p.fcst_fd_amt::double precision
    + p.fcst_pv_amt::double precision AS plannedcommit_noncitycost,
    p.fcst_cnx_amt::double precision + p.fcst_cex_amt::double precision + p.fcst_st_amt::double precision
    + p.fcst_fd_amt::double precision + fcst_pv_amt::double precision
    AS plannedcommit_total
FROM fisa_capitalcommitments AS p
LEFT JOIN dcp_projecttypes_agencies AS b ON trim(p.budget_proj_type) = trim(b.projecttypeabbrev);
