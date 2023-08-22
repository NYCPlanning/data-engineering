--Create Commitments staging table

--with scraped data
DROP TABLE IF EXISTS cpdb_commitments;
CREATE TABLE cpdb_commitments AS (
    WITH summary AS (
        SELECT
            p.ccpversion,
            p.projectid,
            p.budgetline,
            p.plancommdate,
            p.commitmentdescription,
            p.commitmentcode,
            LPAD(p.managingagency, 3, '0') AS magency,
            LPAD(p.managingagency, 3, '0') || p.projectid AS maprojid,
            SUM(citycost::double precision) AS citycost,
            SUM(noncitycost::double precision) AS noncitycost,
            SUM(citycost::double precision + noncitycost::double precision) AS totalcost
        FROM omb_capitalcommitments AS p
        GROUP BY p.ccpversion, p.managingagency, p.projectid, p.budgetline, p.plancommdate, p.commitmentdescription, p.commitmentcode
    )

    SELECT s.*
    FROM summary AS s
);
