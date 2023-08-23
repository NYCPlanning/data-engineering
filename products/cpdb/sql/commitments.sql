--Create Commitments staging table

--with scraped data
DROP TABLE IF EXISTS cpdb_commitments;
CREATE TABLE cpdb_commitments AS (
        WITH summary AS (
	SELECT p.ccpversion,
        LPAD(p.managingagency,3,'0') AS magency,
        p.projectid, LPAD(p.managingagency,3,'0')||p.projectid AS maprojid,
        p.budgetline,
        p.plancommdate,
        p.commitmentdescription,
        p.commitmentcode,
	SUM(citycost::double precision) AS citycost,
        SUM(noncitycost::double precision) AS noncitycost,
        SUM(citycost::double precision + noncitycost::double precision) AS totalcost
	FROM omb_capitalcommitments p
	GROUP BY p.ccpversion, p.managingagency, p.projectid, p.budgetline, p.plancommdate, p.commitmentdescription, p.commitmentcode)
SELECT s.*
FROM summary s);
