--Create Project table view

--with scraped data
DROP TABLE IF EXISTS cpdb_projects;
CREATE TABLE cpdb_projects AS (
    WITH summary AS (
        SELECT
            p.ccpversion,
            p.managingagency AS magency,
            p.projectid,
            p.description,
            p.managingagency || p.projectid AS maprojid,
            ROW_NUMBER() OVER (
                PARTITION BY
                    p.managingagency,
                    p.projectid,
                    p.ccpversion ORDER BY
                    LENGTH(REPLACE(p.description, ' ', '')) DESC
            ) AS rk
        FROM omb_capitalcommitments AS p
    ),

    costs AS (
        SELECT
            ccpversion,
            managingagency,
            projectid,
            managingagency || projectid AS maprojid,
            SUM(citycost::double precision) AS citycost,
            SUM(noncitycost::double precision) AS noncitycost,
            SUM(citycost::double precision + noncitycost::double precision) AS totalcost
        FROM omb_capitalcommitments
        GROUP BY ccpversion, projectid, managingagency
    )

    SELECT
        s.ccpversion,
        s.magency,
        s.projectid,
        s.maprojid,
        s.description,
        c.citycost,
        c.noncitycost,
        c.totalcost,
        a.cape_agencyacronym AS magencyacro,
        a.cape_agency AS magencyname
    FROM summary AS s,
        costs AS c,
        dcp_agencylookup AS a
    WHERE
        s.ccpversion = c.ccpversion
        AND s.rk = 1
        AND s.maprojid = c.maprojid
        AND s.magency = LPAD(a.cape_agencycode, 3, '0')
);
