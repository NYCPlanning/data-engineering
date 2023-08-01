--Create Commitments staging table

--with fisa data
DROP TABLE IF EXISTS cpdb_commitments;
CREATE TABLE cpdb_commitments AS (
        WITH summary AS (
        SELECT 'fisa_'||p.cycle_fy AS ccpversion,
        LPAD(p.managing_agcy_cd, 3, '0') AS magency,
        REPLACE(p.project_id,' ','') AS projectid, 
        LPAD(p.managing_agcy_cd, 3, '0')||REPLACE(p.project_id,' ','') AS maprojid,
        REPLACE(p.budget_proj_type,' ','')||'-'||p.budget_line_id AS budgetline,
        RIGHT(p.planned_commit_date, 2)||'/'||SUBSTRING(p.planned_commit_date from 3 for 2) as plancommdate,
        p.object_name AS commitmentdescription,
        p.object AS commitmentcode,
        p.typ_category AS typc,
        p.typ_category_name AS typcname,
        p.fcst_cnx_amt::double precision AS ccnonexempt,
        p.fcst_cex_amt::double precision AS ccexempt,
        SUM(p.fcst_cnx_amt::double precision + p.fcst_cex_amt::double precision) AS citycost,
        p.fcst_st_amt::double precision AS nccstate,
        p.fcst_fd_amt::double precision AS nccfederal,
        p.fcst_pv_amt::double precision AS nccother,
        SUM(p.fcst_st_amt::double precision + p.fcst_fd_amt::double precision + p.fcst_pv_amt::double precision) AS noncitycost,
        SUM(p.fcst_cnx_amt::double precision + p.fcst_cex_amt::double precision + p.fcst_st_amt::double precision 
                + p.fcst_fd_amt::double precision + fcst_pv_amt::double precision) AS totalcost
        FROM fisa_capitalcommitments p
        GROUP BY p.cycle_fy, LPAD(p.managing_agcy_cd, 3, '0'), REPLACE(p.project_id,' ',''), REPLACE(p.budget_proj_type,' ',''), 
        p.budget_line_id, p.planned_commit_date, p.object_name, p.object, p.typ_category, p.typ_category_name, p.fcst_cnx_amt, 
        p.fcst_cex_amt, p.fcst_st_amt, p.fcst_fd_amt, p.fcst_pv_amt)
SELECT s.*
FROM summary s);