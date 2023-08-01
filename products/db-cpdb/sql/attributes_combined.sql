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
FROM cpdb_projects a
-- spending, commitments, earliest commit or spend, latest commit or spend
LEFT JOIN (
  SELECT  
    x.maprojid,
    min(x.date) mindate,
    max(x.date) maxdate,
    sum(x.commitspend) as totalcommitspend,
    sum(x.commit) as totalcommit,
    sum(x.spend) as totalspend
  FROM (
  SELECT TRIM(LEFT(capital_project,12)) as maprojid,
      issue_date::text as date,
      0 as commit,
      check_amount::double precision as spend,
      check_amount::double precision as commitspend
    FROM capital_spending
    UNION ALL
    SELECT maprojid,
      to_date(plancommdate,'MM/YY')::text as date,
      totalcost as commit,
    0 as spend,
      totalcost as commitspend
    FROM cpdb_commitments
    WHERE ccpversion = :'ccp_v'
  ) x
  GROUP BY x.maprojid
) c ON a.maprojid = c.maprojid
LEFT JOIN (
  SELECT *
  FROM cpdb_dcpattributes
  ) d ON a.maprojid=d.maprojid
);




