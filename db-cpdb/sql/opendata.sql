--All Commitments Tabular
DROP TABLE IF EXISTS cpdb_opendata_commitments;
SELECT 
c.maprojid, 
c.magency,
c.projectid,
p.description, 
c.budgetline,
b.projecttype,
c.plancommdate,
c.commitmentdescription,
c.commitmentcode,
c.typc,
c.typcname,
c.ccnonexempt,
c.ccexempt,
p.citycost AS totalcityplannedcommit,
c.nccstate,
c.nccfederal, 
c.nccother,
p.noncitycost AS totalnoncityplannedcommit,
p.totalcost AS totalplannedcommit,
b.sagencyacro, 
b.sagencyname,
p.magencyacro, 
p.magencyname, 
c.ccpversion
INTO cpdb_opendata_commitments
FROM cpdb_commitments as c
LEFT JOIN cpdb_projects as p ON c.maprojid = p.maprojid
LEFT JOIN cpdb_budgets as b ON c.budgetline = b.budgetline AND c.maprojid = b.maprojid;

--All Projects Tabular
DROP TABLE IF EXISTS cpdb_opendata_projects;
WITH proj AS (
SELECT 
p.maprojid,
p.magency, 
p.projectid, 
p.description, 
p.ccnonexempt,
p.ccexempt, 
p.citycost AS totalcityplannedcommit,
p.nccstate, 
p.nccfederal, 
p.nccother, 
p.noncitycost AS totalnoncityplannedcommit, 
p.totalcost AS totalplannedcommit, 
pc.totalspend,
pc.maxdate, 
pc.mindate,
p.magencyacro,
p.magencyname,
pc.typecategory,
p.ccpversion
FROM cpdb_projects as p,
    cpdb_projects_combined as pc
WHERE p.maprojid = pc.maprojid)
SELECT p.*
INTO cpdb_opendata_projects
FROM proj as p;

--Project Points Table
DROP TABLE IF EXISTS cpdb_opendata_projects_pts;
WITH proj_pts AS (
SELECT 
p.maprojid,
p.magency, 
p.projectid, 
p.description, 
p.ccnonexempt,
p.ccexempt, 
p.citycost AS totalcityplannedcommit,
p.nccstate, 
p.nccfederal, 
p.nccother, 
p.noncitycost AS totalnoncityplannedcommit, 
p.totalcost AS totalplannedcommit, 
pc.totalspend,
pc.maxdate, 
pc.mindate,
p.magencyacro,
p.magencyname,
pc.typecategory,
p.ccpversion,
d.geom
FROM cpdb_projects as p,
cpdb_projects_combined as pc, 
cpdb_dcpattributes as d
WHERE 
p.maprojid = pc.maprojid AND
p.maprojid = d.maprojid AND
ST_GeometryType(geom)= 'ST_MultiPoint')
SELECT p.*
INTO cpdb_opendata_projects_pts
FROM proj_pts as p;

--Project Polygons Table
DROP TABLE IF EXISTS cpdb_opendata_projects_poly;
WITH proj_poly AS (
SELECT 
p.maprojid,
p.magency, 
p.projectid, 
p.description, 
p.ccnonexempt,
p.ccexempt, 
p.citycost AS totalcityplannedcommit,
p.nccstate, 
p.nccfederal, 
p.nccother, 
p.noncitycost AS totalnoncityplannedcommit, 
p.totalcost AS totalplannedcommit, 
pc.totalspend,
pc.maxdate, 
pc.mindate,
p.magencyacro,
p.magencyname,
pc.typecategory,
p.ccpversion,
d.geom
FROM cpdb_projects as p,
cpdb_projects_combined as pc, 
cpdb_dcpattributes as d
WHERE 
p.maprojid = pc.maprojid AND
p.maprojid = d.maprojid AND
ST_GeometryType(geom)= 'ST_MultiPolygon')
SELECT p.*
INTO cpdb_opendata_projects_poly
FROM proj_poly as p;