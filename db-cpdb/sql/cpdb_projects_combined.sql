-- run this script in carto to new create table named cpdb_projects_combined that powers the explorer
SELECT
  a.*,
  b.projecttype,
  b.sagencyacro,
  c.typc,
  c.typcname
FROM cpdb_projects_combined a
-- arrays of project types and sponsor agency acronyms
LEFT JOIN (
  SELECT
    maprojid,
    array_agg(DISTINCT projecttype) AS projecttype,
    array_agg(DISTINCT sagencyacro) AS sagencyacro
  FROM cpdb_budgets
  GROUP BY maprojid
) b ON a.maprojid = b.maprojid
-- only include if working with FISA data
-- arrays of typc
LEFT JOIN (
  SELECT
    maprojid,
    array_agg(DISTINCT typc) AS typc,
    array_agg(DISTINCT typcname) AS typcname
  FROM cpdb_commitments
  GROUP BY maprojid
) c ON a.maprojid = c.maprojid
