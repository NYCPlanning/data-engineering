-- Create a flatfile base on data from FISA
-- find ccpversion to set the date timestamp for the output of the flat file
DROP TABLE IF EXISTS cpdb_projects_spending_date;
CREATE TABLE cpdb_projects_spending_date AS (
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
FROM cpdb_projects a
-- spending, commitments, earliest commit or spend, latest commit or spend
LEFT JOIN (
  SELECT  
    x.maprojid,
    min(x.date) mindate,
    max(x.date) maxdate,
    sum(x.spend) as totalspenddate
  FROM (
  SELECT TRIM(LEFT(capital_project,12)) as maprojid,
      issue_date::text as date,
      check_amount::double precision as spend
    FROM capital_spending
    WHERE LEFT(issue_date, 4)::double precision >= 2014
    -- UNION ALL
    -- SELECT maprojid,
    --   to_date(plancommdate,'MM/YY')::text as date,
    --   totalcost as commit,
    -- 0 as spend,
    --   totalcost as commitspend
    -- FROM cpdb_commitments
    --WHERE ccpversion = :'ccp_v'
  ) x
  GROUP BY x.maprojid
) c ON a.maprojid = c.maprojid
);




