UPDATE fisa_capitalcommitments
SET
    fcst_cnx_amt = (CASE WHEN fcst_cnx_amt ~* '_' THEN '0' ELSE fcst_cnx_amt END),
    fcst_st_amt = (CASE WHEN fcst_st_amt ~* '_' THEN '0' ELSE fcst_st_amt END),
    fcst_cex_amt = (CASE WHEN fcst_cex_amt ~* '_' THEN '0' ELSE fcst_cex_amt END),
    fcst_fd_amt = (CASE WHEN fcst_fd_amt ~* '_' THEN '0' ELSE fcst_fd_amt END),
    fcst_pv_amt = (CASE WHEN fcst_pv_amt ~* '_' THEN '0' ELSE fcst_pv_amt END);

--Create Project table view
-- with fisa data
DROP TABLE IF EXISTS cpdb_projects;
CREATE TABLE cpdb_projects AS (
    WITH summary AS (
        SELECT
            'fisa_' || p.cycle_fy AS ccpversion,
            LPAD(p.managing_agcy_cd, 3, '0') AS magency,
            REPLACE(p.project_id, ' ', '') AS projectid,
            LPAD(p.managing_agcy_cd, 3, '0') || REPLACE(p.project_id, ' ', '') AS maprojid,
            p.short_descr AS description,
            ROW_NUMBER() OVER (
                PARTITION BY
                    LPAD(p.managing_agcy_cd, 3, '0'),
                    REPLACE(p.project_id, ' ', ''),
                    'fisa_' || p.cycle_fy ORDER BY
                    LENGTH(REPLACE(p.short_descr, ' ', '')) DESC
            ) AS rk
        FROM fisa_capitalcommitments AS p
    ),

    costs AS (
        SELECT
            'fisa_' || p.cycle_fy AS ccpversion,
            LPAD(p.managing_agcy_cd, 3, '0') AS magency,
            REPLACE(p.project_id, ' ', '') AS projectid,
            LPAD(p.managing_agcy_cd, 3, '0') || REPLACE(p.project_id, ' ', '') AS maprojid,
            SUM(fcst_cnx_amt::double precision) AS ccnonexempt,
            SUM(fcst_cex_amt::double precision) AS ccexempt,
            SUM(fcst_cnx_amt::double precision + fcst_cex_amt::double precision) AS citycost,
            SUM(fcst_st_amt::double precision) AS nccstate,
            SUM(fcst_fd_amt::double precision) AS nccfederal,
            SUM(fcst_pv_amt::double precision) AS nccother,
            SUM(
                fcst_st_amt::double precision + fcst_fd_amt::double precision + fcst_pv_amt::double precision
            ) AS noncitycost,
            SUM(
                fcst_cnx_amt::double precision
                + fcst_cex_amt::double precision
                + fcst_st_amt::double precision
                + fcst_fd_amt::double precision
                + fcst_pv_amt::double precision
            ) AS totalcost
        FROM fisa_capitalcommitments AS p
        GROUP BY p.cycle_fy, LPAD(p.managing_agcy_cd, 3, '0'), REPLACE(p.project_id, ' ', '')
    )

    SELECT
        s.ccpversion,
        s.magency,
        s.projectid,
        s.maprojid,
        s.description,
        c.ccnonexempt,
        c.ccexempt,
        c.citycost,
        c.nccstate,
        c.nccfederal,
        c.nccother,
        c.noncitycost,
        c.totalcost,
        (CASE
            WHEN UPPER(s.description) LIKE '%BNY%' AND s.magency = '801' THEN 'BNY'
            WHEN LOWER(s.description) LIKE '%governor%s island%' AND s.magency = '801' THEN 'TGI'
            ELSE a.cape_agencyacronym
        END) AS magencyacro,
        (CASE
            WHEN UPPER(s.description) LIKE '%BNY%' AND s.magency = '801' THEN 'Brooklyn Navy Yard'
            WHEN LOWER(s.description) LIKE '%governor%s island%' AND s.magency = '801' THEN 'Trust for Governors Island'
            ELSE a.cape_agency
        END) AS magencyname
    FROM summary AS s,
        costs AS c,
        dcp_agencylookup AS a
    WHERE
        s.ccpversion = c.ccpversion
        AND s.rk = 1
        AND s.maprojid = c.maprojid
        AND s.magency = LPAD(a.cape_agencycode, 3, '0')
);
