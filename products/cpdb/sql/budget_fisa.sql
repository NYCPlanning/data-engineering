--Create Budget staging table

--with fisa data
DROP TABLE IF EXISTS cpdb_budgets;
CREATE TABLE cpdb_budgets AS (
        WITH summary AS (
        SELECT 'fisa_'||p.cycle_fy AS ccpversion,
        LPAD(p.managing_agcy_cd, 3, '0') AS magency,
        REPLACE(p.project_id,' ','') AS projectid,
        LPAD(p.managing_agcy_cd, 3, '0')||REPLACE(p.project_id,' ','') AS maprojid,
        REPLACE(p.budget_proj_type,' ','')||'-'||p.budget_line_id AS budgetline,
        b.projecttype AS projecttype,
        b.agencyacronym AS sagencyacro,
        b.agency AS sagencyname,
        SUM(fcst_cnx_amt::double precision) AS ccnonexempt,
        SUM(fcst_cex_amt::double precision) AS ccexempt,
        SUM(fcst_cnx_amt::double precision + fcst_cex_amt::double precision) AS citycost,
        SUM(fcst_st_amt::double precision) AS nccstate,
        SUM(fcst_fd_amt::double precision) AS nccfederal,
        SUM(fcst_pv_amt::double precision) AS nccother,
        SUM(fcst_st_amt::double precision + fcst_fd_amt::double precision + fcst_pv_amt::double precision) AS noncitycost,
        SUM(fcst_cnx_amt::double precision + fcst_cex_amt::double precision + fcst_st_amt::double precision + fcst_fd_amt::double precision + fcst_pv_amt::double precision) AS totalcost
        FROM fisa_capitalcommitments p
        LEFT JOIN dcp_projecttypes_agencies b ON TRIM(p.budget_proj_type)=TRIM(b.projecttypeabbrev)
        GROUP BY p.cycle_fy, LPAD(p.managing_agcy_cd, 3, '0'), REPLACE(p.project_id,' ',''), REPLACE(p.budget_proj_type,' ',''), p.budget_line_id, b.projecttype, b.agencyacronym, b.agency)
SELECT s.*
FROM summary s);
