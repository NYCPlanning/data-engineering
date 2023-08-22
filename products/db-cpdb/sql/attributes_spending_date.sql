-- Create a flatfile base on data from FISA
-- find ccpversion to set the date timestamp for the output of the flat file
DROP TABLE IF EXISTS cpdb_projects_spending_date;
CREATE TABLE cpdb_projects_spending_date AS (
    WITH c AS (
        SELECT
            x.maprojid,
            min(x.date) AS mindate,
            max(x.date) AS maxdate,
            sum(x.spend) AS totalspenddate
        FROM (
            SELECT
                issue_date::text AS date,
                check_amount::double precision AS spend,
                trim(left(capital_project, 12)) AS maprojid
            FROM capital_spending
            WHERE left(issue_date, 4)::double precision >= 2014
        -- UNION ALL
        -- SELECT maprojid,
        --   to_date(plancommdate,'MM/YY')::text as date,
        --   totalcost as commit,
        -- 0 as spend,
        --   totalcost as commitspend
        -- FROM cpdb_commitments
        --WHERE ccpversion = :'ccp_v'
        ) AS x
        GROUP BY x.maprojid
    )

    SELECT
        a.maprojid,
        a.magencyacro,
        a.magency,
        a.magencyname,
        a.description,
        a.projectid,
        c.mindate,
        c.maxdate,
        c.totalspenddate
    FROM cpdb_projects AS a
    -- spending, commitments, earliest commit or spend, latest commit or spend
    LEFT JOIN c ON a.maprojid = c.maprojid
);
