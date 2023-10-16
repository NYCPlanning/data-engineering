-- Create a flatfile base on data extracted from the Capital Commitment Plan
-- find ccpversion to set the date timestamp for the output of the flat file
DROP TABLE IF EXISTS cpdb_projects_combined;
CREATE TABLE cpdb_projects_combined AS (
    SELECT
        a.ccpversion,
        a.maprojid,
        a.magencyacro,
        a.magency,
        a.magencyname,
        a.description,
        a.projectid,
        c.mindate,
        c.maxdate,
        c.totalcommitspend,
        c.totalcommit,
        c.totalspend
    FROM cpdb_projects AS a
    -- spending, commitments, earliest commit or spend, latest commit or spend
    LEFT JOIN (
        SELECT
            x.maprojid,
            min(x.date) AS mindate,
            max(x.date) AS maxdate,
            sum(x.commitspend) AS totalcommitspend,
            sum(x.commit) AS totalcommit,
            sum(x.spend) AS totalspend
        FROM (
            SELECT
                trim(left(capital_project, 12)) AS maprojid,
                issue_date::text AS date,
                0 AS commit,
                check_amount::double precision AS spend,
                check_amount::double precision AS commitspend
            FROM capital_spending
            UNION ALL
            SELECT
                maprojid,
                to_date(plancommdate, 'MM/YY')::text AS date,
                totalcost AS commit,
                0 AS spend,
                totalcost AS commitspend
            FROM cpdb_commitments
            WHERE ccpversion = :'ccp_v'
        ) AS x
        GROUP BY x.maprojid
    ) AS c ON a.maprojid = c.maprojid
    LEFT JOIN (
        SELECT *
        FROM cpdb_dcpattributes
    ) AS d ON a.maprojid = d.maprojid
);
