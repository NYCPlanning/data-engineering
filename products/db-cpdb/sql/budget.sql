--Create Budget staging table

--with scraped data
DROP TABLE IF EXISTS cpdb_budgets;
CREATE TABLE cpdb_budgets AS (
    WITH summary AS (
        SELECT
            p.ccpversion,
            p.projectid,
            p.budgetline,
            b.projecttype AS projecttype,
            b.agencyacronym AS sagencyacro,
            b.agency AS sagencyname,
            LPAD(p.managingagency, 3, '0') AS magency,
            LPAD(p.managingagency, 3, '0') || p.projectid AS maprojid,
            SUM(citycost::double precision) AS citycost,
            SUM(noncitycost::double precision) AS noncitycost,
            SUM(citycost::double precision + noncitycost::double precision) AS totalcost
        FROM omb_capitalcommitments AS p
        LEFT JOIN dcp_projecttypes_agencies AS b ON SPLIT_PART(p.budgetline, '-', 1) = b.projecttypeabbrev
        GROUP BY p.ccpversion, p.managingagency, p.projectid, p.budgetline, b.projecttype, b.agencyacronym, b.agency
    )

    SELECT s.*
    FROM summary AS s
);
