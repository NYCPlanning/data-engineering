--Process of generating v2 of CPDB where all records are based on October 2016 release of the NYC FY 2017 Capital Commitment Plan

--Some rules for the database
--1) A unique record = ma + project id
--2) All records must be assigned a location status
--3) If a record has a discrete geometry and specific site the geoms must be tied a BBL, BIN, street segment, or park ID

--Step 1
--Run commitments-scrape to get 4 csv files with data from each volume of the October 2016 Capital Commitment Plans
--Merge csvs into one csv titled cpp_output_all.csv and import csv into postgres

--To build the spatial database the following external datasets are needed:
--omb_capitalcommitments_fy17_october16
--bkmappluto
--bxmappluto
--mnmappluto
--qnmappluto
--simappluto
--capeprojects
--capitalprojects
--agencylookup
--dcp_lion_nodes
--dcp_lion_roadbeds
--dcp_schooldistricts
--ddc_capitalprojects_infrastructure
--ddc_capitalprojects_publicbuildings
--doitt_buildingfootprints
--dot_capitalprojects_bridges
--dot_capitalprojects_intersections
--dot_capitalprojects_streetreconstruction
--dpr_capitalprojects
--dpr_parksproperties
--dpr_parksproperties_internal
--facilities
--orr_capitalprojects_lines
--orr_capitalprojects_points
--orr_capitalprojects_polygons

--Step 2 
--Create database tables

ALTER TABLE com1_17alloutput RENAME TO omb_capitalcommitments;
--Create Project staging table
--Extract unique recrods based on ma and project id, and only include the longest description. Create a new table 
DROP TABLE IF EXISTS projectstaging;
CREATE TABLE projectstaging AS( WITH summary AS (
	SELECT p.managingagency AS magency, p.projectid, p.managingagency||p.projectid AS maprojid, p.description, 
	ROW_NUMBER() OVER(PARTITION BY p.managingagency, p.projectid
	ORDER BY length(replace(p.description, ' ', '')) DESC) AS rk
	FROM omb_capitalcommitments p)
SELECT s.*
FROM summary s
WHERE s.rk = 1);
--With the subset of records in the projectstaging table and add a series of columns
ALTER TABLE projectstaging
DROP rk;
ALTER TABLE projectstaging
ADD magencyacro text;
ALTER TABLE projectstaging
ADD magencyname text;
ALTER TABLE projectstaging
ADD citycost double precision;
ALTER TABLE projectstaging
ADD noncitycost double precision;
ALTER TABLE projectstaging
ADD totalcost double precision;
--Add cost data per project in projectstaging table
WITH costs AS (SELECT managingagency, projectid, SUM(citycost::double precision)*1000 AS citycost, SUM(noncitycost::double precision)*1000 AS noncitycost, SUM(citycost::double precision + noncitycost::double precision)*1000 AS totalcost
FROM omb_capitalcommitments 
GROUP BY managingagency, projectid)
UPDATE projectstaging SET citycost = costs.citycost, noncitycost = costs.noncitycost, totalcost = costs.totalcost
FROM costs
WHERE projectstaging.magency||' '||projectstaging.projectid=costs.managingagency||' '||costs.projectid;
--Update magency to be left padded with 0s
UPDATE projectstaging SET magency = LPAD(magency, 3, '0');
--Add agency names and acronyms in projectstaging table 
WITH agency AS (SELECT a.magency, a.projectid, b.*
	FROM projectstaging a LEFT JOIN dcp_agencylookup b ON a.magency=LPAD(b.agencycode, 3, '0'))
UPDATE projectstaging SET magencyacro = agency.agencyacronym, magencyname = agency.agency
FROM agency
WHERE projectstaging.magency||' '||projectstaging.projectid=agency.magency||' '||agency.projectid;

--Create Budget staging table
DROP TABLE IF EXISTS budgetstaging;
CREATE TABLE budgetstaging AS( WITH summary AS (
	SELECT LPAD(p.managingagency,3,'0') AS magency, p.projectid, LPAD(p.managingagency,3,'0')||p.projectid AS maprojid, p.budgetline, b.projecttype AS projecttype, b.agencyacronym AS sagencyacro, b.agency AS sagencyname,
	SUM(citycost::double precision)*1000 AS citycost, SUM(noncitycost::double precision)*1000 AS noncitycost, SUM(citycost::double precision + noncitycost::double precision)*1000 AS totalcost
	FROM omb_capitalcommitments p
	LEFT JOIN dcp_projecttypes_agencies b ON split_part(p.budgetline, '-', 1)=b.projecttypeabbrev
	GROUP BY p.managingagency, p.projectid, p.budgetline, b.projecttype, b.agencyacronym, b.agency)
SELECT s.*
FROM summary s);

--Create Commitments staging table
DROP TABLE IF EXISTS commitmentstaging;
CREATE TABLE commitmentstaging AS( WITH summary AS (
	SELECT LPAD(p.managingagency,3,'0') AS magency, p.projectid, LPAD(p.managingagency,3,'0')||p.projectid AS maprojid, p.budgetline, p.plancommdate, p.costdescription,
	SUM(citycost::double precision)*1000 AS citycost, SUM(noncitycost::double precision)*1000 AS noncitycost, SUM(citycost::double precision + noncitycost::double precision)*1000 AS totalcost
	FROM omb_capitalcommitments p
	GROUP BY p.managingagency, p.projectid, p.budgetline, p.plancommdate, p.costdescription)
SELECT s.*
FROM summary s);

--Create DCP Attributes staging table
DROP TABLE IF EXISTS dcpattributestaging;
CREATE TABLE dcpattributestaging AS( 
SELECT magency, projectid, maprojid, description FROM projectstaging
);
ALTER TABLE dcpattributestaging
ADD locationstatus text;
ALTER TABLE dcpattributestaging
ADD bbl text;
ALTER TABLE dcpattributestaging
ADD bin text;
ALTER TABLE dcpattributestaging
ADD segmentid text;
ALTER TABLE dcpattributestaging
ADD parkid text;
ALTER TABLE dcpattributestaging
ADD geomsource text;
ALTER TABLE dcpattributestaging
ADD sourcedataset text;
ALTER TABLE dcpattributestaging
ADD sourceagency text;
ALTER TABLE dcpattributestaging
ADD dateedited timestamp;
ALTER TABLE dcpattributestaging
ADD geom geometry;


--Start adding geometries
--Join agency geometries data onto DCP Attributes staging by FMS ID
WITH master AS (
SELECT a.magency, a.projectid, b.maprojid, b.sourcedata, b.source, b.geom
FROM dcpattributestaging a, capeprojects b
WHERE a.magency||' '||a.projectid=b.maprojid AND b.cpid NOT IN (SELECT cpid FROM capeprojects WHERE fyconend < 2015 AND source = 'DDC') AND b.geom IS NOT NULL
)

UPDATE dcpattributestaging SET geom=master.geom, sourcedataset=master.sourcedata, geomsource='Agency Data', sourceagency=master.source
FROM master
WHERE dcpattributestaging.magency||' '||dcpattributestaging.projectid=master.maprojid AND dcpattributestaging.geom IS NULL;

--Join agency geometries data onto DCP Attributes staging by project name
WITH master AS (
SELECT a.magency, a.projectid, b.maprojid, b.sourcedata, b.source, a.description, b.name, b.geom
FROM dcpattributestaging a, capeprojects b
WHERE a.geom IS NULL AND b.geom IS NOT NULL AND a.magency <> '850'
AND (upper(a.description) NOT LIKE '%CITYWIDE MILLING%' OR upper(a.description) NOT LIKE '%MULTI-SITE%')
AND upper(a.description) LIKE upper(b.name)
AND b.cpstatus <> 'Complete'
)

UPDATE dcpattributestaging SET geom=master.geom, sourcedataset=master.sourcedata, geomsource='Agency Data', sourceagency=master.source
FROM master
WHERE dcpattributestaging.magency=master.magency AND dcpattributestaging.projectid=master.projectid AND dcpattributestaging.geom IS NULL; 

--Join DDC infrastructure agency geometries data onto DCP Attributes staging by FMS ID
WITH master AS (WITH consolidated AS (SELECT a.projectid, a.sponsor, a.managing, a.type, a.estatcompt, a.constr_sta, a.constr_com, a.fyconstcom, a.contactnam, ST_Multi(ST_UNION(a.geom)) AS geom 
FROM ddc_capitalprojects_infrastructure a
GROUP BY a.projectid, a.sponsor, a.managing, a.type, a.estatcompt, a.constr_sta, a.constr_com, a.fyconstcom, a.contactnam)

SELECT a.magency, a.projectid, a.description, a.locationstatus, ST_SetSRID(b.geom, 26918) AS geom
FROM dcpattributestaging a, consolidated b
WHERE a.projectid=b.projectid AND a.geom IS NULL AND b.geom IS NOT NULL)

UPDATE dcpattributestaging SET geom=master.geom, sourcedataset='ddc_capitalprojects_infrastructure', geomsource='Agency Data', sourceagency='DDC'
FROM master
WHERE dcpattributestaging.magency||' '||dcpattributestaging.projectid=master.magency||' '||master.projectid AND dcpattributestaging.geom IS NULL;
--Join DDC buildings agency geometries data onto DCP Attributes staging by FMS ID
WITH master AS (WITH consolidated AS (SELECT a.projectid, a.sponsor, a.managing, a.type, a.estatcompt, a.constr_sta, a.constr_com, a.fyconstcom, a.contactnam, ST_Multi(ST_UNION(a.geom)) AS geom 
FROM ddc_capitalprojects_publicbuildings a
GROUP BY a.projectid, a.sponsor, a.managing, a.type, a.estatcompt, a.constr_sta, a.constr_com, a.fyconstcom, a.contactnam)

SELECT a.magency, a.projectid, a.description, a.locationstatus, ST_SetSRID(b.geom, 26918) AS geom
FROM dcpattributestaging a, consolidated b
WHERE a.projectid=b.projectid AND a.geom IS NULL AND b.geom IS NOT NULL)

UPDATE dcpattributestaging SET geom=master.geom, sourcedataset='ddc_capitalprojects_publicbuildings', geomsource='Agency Data', sourceagency='DDC'
FROM master
WHERE dcpattributestaging.magency||' '||dcpattributestaging.projectid=master.magency||' '||master.projectid AND dcpattributestaging.geom IS NULL;

--Join Park geoms to DPR records via park id
WITH master AS(
WITH filtered AS (
SELECT regexp_replace(regexp_matches(description, '[BMQRX][0-9][0-9][0-9]')::text,'[^0-9a-zA-Z]+','','g')::text AS dprparkid, *
FROM dcpattributestaging a
WHERE magency = '846')

SELECT a.magency, a.projectid, b.geom FROM filtered a
LEFT JOIN dpr_parksproperties_internal b ON a.dprparkid=b.gispropnum::text
WHERE b.geom IS NOT NULL)

UPDATE dcpattributestaging SET geom=master.geom, geomsource='Algorithm', sourcedataset='dpr_parksproperties_internal', sourceagency='DPR'
FROM master
WHERE dcpattributestaging.magency=master.magency AND dcpattributestaging.projectid=master.projectid  AND dcpattributestaging.geom IS NULL;

--Join Park geoms to records via park name round 1 - like statements
WITH master AS(
SELECT a.magency, a.projectid, a.description, b.name311, b.geom
FROM dcpattributestaging a, dpr_parksproperties_internal b
WHERE 
a.geom IS NULL AND a.magency = '846' AND a.description NOT LIKE '%&%' AND a.description NOT LIKE '%/%' AND b.name311 LIKE '%'||' '||'%'
AND 
(upper(a.description) LIKE upper('%' || regexp_replace(b.name311, 'Park|park|PARK|Playground|playground|PLAYGROUND|Triangle|triangle|TRIANGLE|Garden|garden|GARDEN|Lot|lot|LOT|-|--|Sitting|sitting|SITTING|Area|area|AREA|Green|green|GREEN|Pier|pier|PIER|Life|life|LIFE|Of|of|OF|The|the|THE|Asser Levy|asser levy|ASSER LEVY', 'xy') || '%'))
)
UPDATE dcpattributestaging SET geomsource = 'Algorithm', sourcedataset='dpr_parksproperties_internal', sourceagency='DPR', geom=master.geom
FROM master
WHERE dcpattributestaging.magency=master.magency AND dcpattributestaging.projectid=master.projectid AND dcpattributestaging.geom IS NULL;

--Join Park geoms to records via park name round 2 - like statements
WITH master AS (
SELECT a.magency, a.projectid, a.description, b.name311, b.geom FROM dcpattributestaging a, dpr_parksproperties_internal b
WHERE a.magency = '846' AND a.geom IS NULL AND '%'||upper(a.description)||'%' LIKE '% '||upper(b.name311)||'%' 
AND upper(b.name311) LIKE '% %'AND a.description NOT LIKE '% & %'
--these values may need to change depending on how projects and parks are matching
AND upper(b.name311) NOT LIKE 'SITTING AREA' AND upper(b.name311) NOT LIKE '%BRIDGE%' AND upper(b.name311) NOT LIKE 'LEVY%' AND upper(b.name311) NOT LIKE 'TERRACE%'
AND upper(a.description) NOT LIKE '%FROM%TO%'
--AND upper(b.name311) NOT LIKE '%ARMY%' AND upper(b.name311) NOT LIKE '%HUDSON%' AND upper(b.name311) NOT LIKE 'LEVY PLAYGROUND' 
--AND (upper(b.name311) NOT LIKE 'ALLEY' AND upper(b.name311) NOT LIKE 'BATTERY' AND upper(b.name311) NOT LIKE '-' AND upper(b.name311) NOT LIKE 'LOT')
ORDER BY b.name311)
UPDATE dcpattributestaging SET geomsource = 'Algorithm', sourcedataset='dpr_parksproperties_internal', sourceagency='DPR', geom=master.geom
FROM master
WHERE dcpattributestaging.magency=master.magency AND dcpattributestaging.projectid=master.projectid AND dcpattributestaging.geom IS NULL;

--Join Park geoms to records via park name round 3 - fuzzy string matching
WITH master AS(
SELECT a.magency, a.projectid, a.description, b.name311, b.geom
FROM dcpattributestaging a, dpr_parksproperties_internal b
WHERE 
a.geom IS NULL AND a.magency = '846' AND a.description NOT LIKE '%&%' AND a.description NOT LIKE '%/%' AND b.name311 LIKE '%'||' '||'%'
AND (
levenshtein(a.description, regexp_replace(b.name311, 'Park|park|PARK|Playground|playground|PLAYGROUND|Triangle|triangle|TRIANGLE|Garden|garden|GARDEN|Lot|lot|LOT|-|--|Sitting|sitting|SITTING|Area|area|AREA|Green|green|GREEN|Pier|pier|PIER|Life|life|LIFE|Of|of|OF|The|the|THE|Asser Levy|asser levy|ASSER LEVY', 'blank')) <=4
))
UPDATE dcpattributestaging SET geomsource = 'Algorithm', sourcedataset='dpr_parksproperties_internal', sourceagency='DPR', geom=master.geom
FROM master
WHERE dcpattributestaging.magency=master.magency AND dcpattributestaging.projectid=master.projectid AND dcpattributestaging.geom IS NULL;

--Attaching park geoms to records via park name second round
WITH master AS (
--UPDATE commitments SET geomsource = 'AD Sprint', locationstatus = 'discrete'
SELECT a.magency, a.projectid, a.description, b.name311, b.geom FROM dcpattributestaging a, dpr_parksproperties_internal b
WHERE '%'||upper(a.description)||'%' LIKE '% '||upper(b.name311)||'%' AND a.geom IS NULL
AND upper(b.name311) LIKE '% %'AND a.description NOT LIKE '% & %'
AND upper(b.name311) NOT LIKE '%ARMY%' AND upper(b.name311) NOT LIKE '%HUDSON%' AND upper(b.name311) NOT LIKE 'LEVY PLAYGROUND' AND upper(b.name311) NOT LIKE 'SITTING AREA' 
ORDER BY b.name311)
--AND (upper(b.name311) NOT LIKE 'ALLEY' AND upper(b.name311) NOT LIKE 'BATTERY' AND upper(b.name311) NOT LIKE '-' AND upper(b.name311) NOT LIKE 'LOT')

UPDATE commitments SET geomsource = 'DPR Park names', locationstatus = 'discrete', geom=master.geom
FROM master
WHERE commitments.managingagency=master.managingagency AND commitments.projectid=master.projectid AND commitments.geom IS NULL AND commitments.locationstatus IS NULL;

--Attaching park geoms to records via park name third round
WITH master AS (
SELECT a.magency, a.projectid, a.description, b.signname, b.gispropnum, b.geom FROM dcpattributestaging a, dpr_parksproperties_internal b
WHERE a.geom IS NULL AND a.locationstatus IS NULL
AND a.magency = '846'
AND '%'||upper(a.description)||'%' LIKE '%'||upper(b.signname)||'%'
AND b.signname LIKE '% %'
AND b.geom IS NOT NUlL
)
UPDATE commitments SET geomsource = 'DPR Park names', locationstatus = 'discrete', geom=master.geom, parkid=master.gispropnum
FROM master
WHERE commitments.managingagency=master.managingagency AND commitments.projectid=master.projectid AND commitments.geom IS NULL AND commitments.locationstatus IS NULL;


--Add geoms created by DCP Sprint via FMS ID
WITH master AS (SELECT a.managingagency, a.projectid, a.description, b.locationstatus, b.geom, b.maprojectid FROM commitments a, capitalprojects b
WHERE a.managingagency||' '||a.projectid=b.maprojectid AND a.geom IS NULL AND b.geom IS NOT NULL)

UPDATE commitments SET geom=master.geom, geomsource='DCP Sprint', locationstatus=master.locationstatus
FROM master
WHERE commitments.managingagency=master.managingagency AND commitments.projectid=master.projectid AND commitments.geom IS NULL AND master.geom IS NOT NULL;

--Add location status from DCP Sprint via FMS ID
WITH master AS (SELECT a.managingagency, a.projectid, a.description, b.locationstatus, b.geom, b.maprojectid FROM commitments a, capitalprojects b
WHERE a.managingagency||' '||a.projectid=b.maprojectid AND a.geom IS NULL AND b.geom IS NULL AND b.locationstatus IS NOT NULL)

UPDATE commitments SET geomsource='DCP Sprint', locationstatus=master.locationstatus
FROM master
WHERE commitments.managingagency=master.managingagency AND commitments.projectid=master.projectid AND commitments.geom IS NULL AND master.geom IS NULL AND master.locationstatus IS NOT NULL;

--Add geoms from facilities database via name matching (850)
WITH master AS(
WITH singlename AS (WITH filtered AS 
	(SELECT facilityname, COUNT(facilityname) AS namecount 
	FROM facilities 
	GROUP BY facilityname) SELECT facilityname FROM filtered WHERE namecount = 1 )

SELECT a.managingagency, a.projectid, a.description, b.facilityname, b.geom FROM commitments a, facilities b
WHERE a.geom IS NULL AND a.locationstatus IS NULL AND a.managingagency = '850' 
--Also works for 801, 806, 126, 819, 57, 72, 858, 827, 71, 56, 816, 125, and 998 agency codes
AND b.facilityname LIKE '%'||' '||'%'||' '||'%' AND a.description NOT LIKE '%&%'
AND upper(a.description) LIKE '%' || upper(regexp_replace(b.facilityname, 'Park|park|PARK|Playground|playground|PLAYGROUND|Triangle|triangle|TRIANGLE|Garden|garden|GARDEN|Lot|lot|LOT|-|--|Freedom|freedom|FREEDOM|Butterfly|butterfly|BUTTERFLY|Animal|animal|Animal|Life|life|LIFE|Mall|mall|MALL', 'null')) || '%'
AND b.facilityname IN (SELECT facilityname FROM singlename)
)

UPDATE commitments SET geomsource = 'Facilities database', locationstatus = 'discrete', geom=master.geom
FROM master
WHERE commitments.managingagency=master.managingagency AND commitments.projectid=master.projectid AND commitments.geom IS NULL AND master.geom IS NOT NULL;
--Did not work perfect for 
--801, "Food Bank of New York - 355 Food Center Drive", "THE JOSEPH P. ADDABBO FAMILY HEALTH CENTER", 
--850, "UNION COMMUNITY HEALTH CENTER - MOBILE DENTAL VAN"

--Query for checking commitments and facilities were properly matched
--SELECT a.managingagency, a.projectid, a.description, b.facilityname FROM commitments a, facilities b
--WHERE upper(a.description) LIKE '%' || upper(regexp_replace(b.facilityname, 'Park|park|PARK|Playground|playground|PLAYGROUND|Triangle|triangle|TRIANGLE|Garden|garden|GARDEN|Lot|lot|LOT|-|--|Freedom|freedom|FREEDOM|Butterfly|butterfly|BUTTERFLY|Animal|animal|Animal|Life|life|LIFE|Mall|mall|MALL', 'null')) || '%'
--AND a.geomsource = 'Facilities database'
--AND b.geom=a.geom
--GROUP BY a.managingagency, a.projectid, a.description, b.facilityname
--ORDER BY b.facilityname ASC;

--Add geometries to Libraries using the facilities database
WITH master AS (SELECT a.managingagency, a.projectid, a.description, b.facilityname, b.geom FROM commitments a, (SELECT * FROM facilities WHERE facilitygroup = 'Libraries') b
WHERE locationstatus IS NULL
AND (a.managingagency = '39' OR a.managingagency = '37' OR a.managingagency = '38' OR a.managingagency = '35')
AND upper(a.description) NOT LIKE '%AND%'
AND '%'||upper(a.description)||'%' LIKE '%'||upper(b.facilityname)||'%')

UPDATE commitments SET locationstatus='discrete', geom=master.geom
FROM master
WHERE commitments.managingagency=master.managingagency AND commitments.projectid=master.projectid AND commitments.geom IS NULL; 

--Update location status to nondiscrete for multisite projects
WITH master AS(
SELECT * FROM commitments WHERE geom IS NULL AND locationstatus IS NULL
AND (upper(description) LIKE '%MULTISITE%' OR upper(description) LIKE '%MULTI-SITE%' OR upper(description) LIKE '%MULTI SITE%'))

UPDATE commitments SET locationstatus='nondiscrete', geomsource='Amanda Sprint'
FROM master
WHERE commitments.managingagency=master.managingagency AND commitments.projectid=master.projectid AND commitments.geom IS NULL;

--Label citywide projects as nondiscrete
UPDATE commitments SET geomsource = 'AD Sprint', locationstatus = 'nondiscrete'
WHERE
geom IS NULL AND locationstatus IS NULL
AND (upper(description) LIKE '%CITYWIDE%' OR upper(description) LIKE '%CITY WIDE%' OR upper(description) LIKE '%CITY-WIDE%');

--Mark descriptions wth "systemwide" as nondescrite
WITH master AS(
SELECT * FROM commitments WHERE geom IS NULL AND locationstatus IS NULL
AND (upper(description) LIKE'%SYSTEMWIDE%' OR upper(description) LIKE'%SYSTEM WIDE%' OR upper(description) LIKE'%SYSTEM-WIDE%')
)
UPDATE commitments SET locationstatus='nondiscrete'	
FROM master
WHERE commitments.managingagency=master.managingagency AND commitments.projectid=master.projectid AND commitments.geom IS NULL AND commitments.locationstatus IS NULL;

--Update street trees to nondiscrete
UPDATE commitments SET geomsource = 'AD Sprint', locationstatus = 'nondiscrete'
WHERE
geom IS NULL AND locationstatus IS NULL
AND
managingagency = '846'
AND upper(description) LIKE '%STREET TREE%'
AND upper(description) NOT LIKE '%SUPERVISION%';
--Other key words for managingagency = '846' to make nondiscrete
--REQUIREMENTS, BOROUGH, SITES, REFOREST, VARIOUS

--Mark forms of transportation as nonspatial 
UPDATE commitments SET locationstatus = 'nonspatial'
WHERE (description LIKE'%TRUCK%' OR description LIKE'%Truck%' OR description LIKE'%truck%' OR
	description LIKE'%Boat%' OR description LIKE'%BOAT%' OR description LIKE'%boat%')
	AND locationstatus IS NULL AND geom IS NULL;

--Make HPD Programs Nonspatial
UPDATE commitments SET geomsource = 'AD Sprint', locationstatus = 'nonspatial'
--SELECT a.managingagency, a.projectid, a.description FROM commitments a
WHERE
geom IS NULL AND locationstatus IS NULL AND parkid IS NULL AND bbl IS NULL
AND managingagency = '806'
AND upper(description) LIKE '%PROGRAM%';

--stop gap - research happened to add location status to all records and give geoms to discrete records --

--Update location status for multi geoms with more than one geom
SELECT *, ST_NumGeometries(geom) FROM commitments 
WHERE geom IS NOT NULL AND ST_NumGeometries(geom) > 1 AND (locationstatus IS NULL OR locationstatus = 'nonspatial') AND ST_GeometryType(geom) LIKE '%Poly%';

--Add pluto geoms to records via bbl once bbl is manually assigned
WITH master AS (SELECT a.managingagency, a.projectid, a.bbl, (trunc(b.bbl, 0))::text, b.geom FROM commitments a, qnmappluto b
WHERE a.bbl IS NOT NULL AND a.geom IS NULL AND a.bbl=(trunc(b.bbl, 0))::text)

UPDATE commitments SET geom=master.geom
FROM master
WHERE commitments.managingagency=master.managingagency AND commitments.projectid=master.projectid AND commitments.geom IS NULL AND commitments.bbl=master.bbl; 

--Add bin geoms via bin one bin is manually assigned
WITH master AS (SELECT a.managingagency, a.projectid, a.bin, (trunc(b.bin, 0))::text, b.geom FROM commitments a, doitt_buildingfootprints b
WHERE a.bin IS NOT NULL AND a.geom IS NULL AND a.bin=(trunc(b.bin::bigint::text, 0))::text)

UPDATE commitments SET geom=ST_SetSRID(master.geom,26918)
FROM master
WHERE commitments.managingagency=master.managingagency AND commitments.projectid=master.projectid AND commitments.geom IS NULL AND commitments.bin=master.bin; 

--Add park geoms to records via park id once park id is manually assigned
WITH master AS (SELECT a.managingagency, a.projectid, a.parkid, b.gispropnum, b.geom FROM commitments a, dpr_parksproperties_internal b
WHERE a.parkid IS NOT NULL AND a.geom IS NULL AND a.parkid=b.gispropnum)

UPDATE commitments SET geom=master.geom
FROM master
WHERE commitments.managingagency=master.managingagency AND commitments.projectid=master.projectid AND commitments.geom IS NULL AND commitments.parkid=master.parkid; 

--Add node geoms from LION one node id is manually assigned
WITH master AS (SELECT a.maprojid, b.geom FROM commitments a
LEFT JOIN dcp_lion_nodes b ON a.segmentid::text=b.nodeid::text
WHERE a.segmentid IS NOT NULL AND a.geom IS NULL AND a.segmentid::text=b.nodeid::text)

UPDATE commitments SET geom=ST_SetSRID(master.geom,26918), geomsource='AD Sprint'
FROM master
WHERE commitments.maprojid=master.maprojid AND commitments.geom IS NULL;

--Change segment id from text to array
ALTER TABLE commitments ALTER segmentid type text[] USING sting_to_array(segmentid,',');

--Add segment geoms from LION
WITH master AS (SELECT a.maprojid, ST_Union(b.geom) AS geom FROM commitments a, unnest(a.segmentid) AS singlesegmentid
LEFT JOIN dcp_lion_roadbeds b ON singlesegmentid=b.segmentid
WHERE a.geom IS NULL AND a.segmentid IS NOT NULL
GROUP BY a.maprojid)

UPDATE commitments SET geom=ST_SetSRID(master.geom,26918), geomsource='AD Sprint'
FROM master
WHERE commitments.maprojid=master.maprojid AND commitments.geom IS NULL;

--Add bbls to records that have geom but no bbl or park id
SELECT UpdateGeometrySRID('commitments','geom',4326);
SELECT UpdateGeometrySRID('simappluto','geom',4326);
--Repeat for each version of mappluto if there is not one version
DROP INDEX IF EXISTS dcp_mappluto_x;
CREATE INDEX dcp_mappluto_x ON simappluto USING GIST (geom);
DROP INDEX IF EXISTS commitments_x;
CREATE INDEX commitments_x ON commitments USING GIST (geom);
WITH master AS (
SELECT a.managingagency, a.projectid, a.geom, (trunc(b.bbl, 0))::text AS bbl
FROM commitments a, simappluto b WHERE a.bbl IS NULL AND a.parkid IS NULL AND a.geom IS NOT NULL
AND ST_Intersects(ST_Centroid(a.geom), b.geom)
)

UPDATE commitments SET bbl=master.bbl, geomsource='AD Sprint'
FROM master
WHERE commitments.managingagency=master.managingagency AND commitments.projectid=master.projectid AND commitments.bbl IS NULL; 

--Add park ids to records that have geom but no bbl or park id
SELECT UpdateGeometrySRID('commitments','geom',26918);
SELECT UpdateGeometrySRID('dpr_parksproperties_internal','geom',26918);

DROP INDEX IF EXISTS dpr_parksproperties_internal_x;
CREATE INDEX dpr_parksproperties_internal_x ON dpr_parksproperties_internal USING GIST (geom);
DROP INDEX IF EXISTS commitments_x;
CREATE INDEX commitments_x ON commitments USING GIST (geom);
WITH master AS (
SELECT a.managingagency, a.projectid, a.geom, b.gispropnum AS parkid
FROM commitments a, dpr_parksproperties_internal b WHERE a.bbl IS NULL AND a.parkid IS NULL AND a.geom IS NOT NULL
AND ST_Intersects(ST_Centroid(a.geom), b.geom)
)
UPDATE commitments SET parkid=master.parkid, geomsource='AD Sprint'
FROM master
WHERE commitments.managingagency=master.managingagency AND commitments.projectid=master.projectid AND commitments.parkid IS NULL;

--Update lines to be polygons
--SELECT DISTINCT ST_GeometryType(geom) FROM commitments
UPDATE commitments
SET geom=ST_Buffer(geom::geography, 15)::geometry
WHERE ST_GeometryType(geom) = 'ST_MultiLineString';

--Update geom to be multi
UPDATE commitments
SET geom=ST_Multi(geom)
WHERE ST_GeometryType(geom)='ST_Polygon';

--Export shapefiles - done in terminal
pgsql2shp -f dcpattributespoints -h localhost -u adoyle postgis-test "SELECT * FROM dcpattributes WHERE ST_GeometryType(geom)='ST_MultiPoint'"
--pgsql2shp -f commitmentslines -h localhost -u adoyle postgis-test "SELECT * FROM commitments WHERE ST_GeometryType(geom)='ST_MultiLineString'"
pgsql2shp -f dcpattributespolygons -h localhost -u adoyle postgis-test "SELECT * FROM dcpattributes WHERE ST_GeometryType(geom)='ST_MultiPolygon'"

--SCA
--Attach district geoms onto SCA projects
WITH master AS (SELECT a.id, b.geom  FROM sca_capitalprojects_nov16 a
LEFT JOIN dcp_schooldistricts b ON b.schooldist::text=a.district
WHERE a.geom IS NULL AND b.geom IS NOT NULL AND a.locationstatus = 'tbd')

UPDATE sca_capitalprojects_nov16 SET geom=ST_SetSRID(master.geom,4326)
FROM master
WHERE sca_capitalprojects_nov16.id=master.id AND sca_capitalprojects_nov16.geom IS NULL AND sca_capitalprojects_nov16.locationstatus='tbd';

--Export shapefiles - done in terminal
pgsql2shp -f scapoints -h localhost -u adoyle postgis-test "SELECT * FROM sca_capitalprojects_nov16 WHERE ST_GeometryType(geom)='ST_MultiPoint'"
--pgsql2shp -f scalines -h localhost -u adoyle postgis-test "SELECT * FROM sca_capitalprojects_nov16 WHERE ST_GeometryType(geom)='ST_MultiLineString'"
pgsql2shp -f scapolygons -h localhost -u adoyle postgis-test "SELECT * FROM sca_capitalprojects_nov16 WHERE ST_GeometryType(geom)='ST_MultiPolygon'"


pgsql2shp -f facilitiestesttest -h localhost -u adoyle postgis-test "SELECT * FROM facilities LIMIT 10"

pgsql2shp -f ddc_capitalprojects_infrastructure_SubsetDigOnceAnalysis -h localhost -u adoyle postgis-test "SELECT * FROM ddc_capitalprojects_infrastructure WHERE EXTRACT(YEAR FROM constr_com) > 1999 AND (type = 'Water' OR type = 'Sewer' OR type = 'Street Resurfacing')"

--^^done


--qeries

--create summary table by geom and location status
COPY( WITH total AS(WITH master AS (SELECT a.*,b.* FROM commitments a LEFT JOIN coib_agencycodelookup b ON a.managingagency=b.agencycode)
SELECT a.managingagency, a.acronym, a.locationstatus, COUNT(a.*) AS total
FROM master a GROUP BY a.locationstatus, a.managingagency, a.acronym )

SELECT * FROM total a
LEFT JOIN (
SELECT locationstatus, managingagency, COUNT(*) AS hasgeom FROM commitments 
WHERE geom IS NOT NULL OR bbl IS NOT NULL or bin IS NOT NULL OR parkid IS NOT NULL OR segmentid IS NOT NULL
GROUP BY locationstatus, managingagency ) AS b
ON a.locationstatus=b.locationstatus AND a.managingagency=b.managingagency )
TO '/Users/adoyle/Downloads/commitmentssummaryJan4.csv' DELIMITER ',' CSV HEADER;


--analyses -- $ and count summary statistics for 5 year capital plan for locationstatus and geom source
WITH totalsum AS (SELECT COUNT(DISTINCT a.maprojid) AS totalcount, SUM(a.citycost) AS totalcitycost, SUM(a.noncitycost) AS totalnoncitycost, SUM(a.totalcost) AS totalcost, b.geomsource, b.locationstatus
FROM commitscommitments a
LEFT JOIN commitments b ON a.managingagency=b.managingagency AND a.projectid=b.projectid
WHERE LEFT(plancommdate,2) < '21'
GROUP BY b.geomsource, b.locationstatus)

SELECT * FROM totalsum
LEFT JOIN
(
SELECT COUNT(DISTINCT a.maprojid) AS hasgeomcount, SUM(a.citycost) AS hasgeomcitycost, SUM(a.noncitycost) AS hasgeomnoncitycost, SUM(a.totalcost) AS hasgeomtotalcost, b.geomsource, b.locationstatus
FROM commitscommitments a
LEFT JOIN commitments b ON a.managingagency=b.managingagency AND a.projectid=b.projectid
WHERE LEFT(plancommdate,2) < '21'
AND geom IS NOT NULL
GROUP BY b.geomsource, b.locationstatus
) hasgeom
ON totalsum.geomsource=hasgeom.geomsource AND totalsum.locationstatus=hasgeom.locationstatus



---drafts
--attaching park geoms to DPR records via park name - not worth it because only returns 6 records
--v1
SELECT a.managingagency, a.projectid, a.description, b.name311, b.geom FROM commitments a, dpr_parksproperties_internal b
WHERE a.managingagency = '846' AND a.geom IS NULL AND a.locationstatus IS NULL
AND (a.description = b.name311
OR a.description LIKE '%' || regexp_replace(b.name311, 'Park|park|PARK|Playground|playground|PLAYGROUND|Triangle|triangle|TRIANGLE|Garden|garden|GARDEN|Lot|lot|LOT|-|--', 'null') || '%'
OR levenshtein(a.description, regexp_replace(b.name311, 'Park|park|PARK|Playground|playground|PLAYGROUND|Triangle|triangle|TRIANGLE|Garden|garden|GARDEN|Lot|lot|LOT|-|--', 'null')) <=4)
AND b.name311 NOT IN (WITH filtered AS (SELECT name311, COUNT(name311) AS namecount FROM dpr_parksproperties_internal GROUP BY name311) SELECT name311 FROM filtered WHERE namecount >= 2 )
--v2
SELECT a.managingagency, a.projectid, a.description, b.name311, b.geom FROM commitments a, dpr_parksproperties_internal b
WHERE a.managingagency = '846' AND a.geom IS NULL AND a.locationstatus IS NULL
AND (upper(a.description) = upper(b.name311)
OR upper(a.description) LIKE upper('%' || regexp_replace(b.name311, 'Park|park|PARK|Playground|playground|PLAYGROUND|Triangle|triangle|TRIANGLE|Garden|garden|GARDEN|Lot|lot|LOT|Strip|-|--', 'null') || '%')
OR levenshtein(upper(a.description), upper(regexp_replace(b.name311, 'Park|park|PARK|Playground|playground|PLAYGROUND|Triangle|triangle|TRIANGLE|Garden|garden|GARDEN|Lot|lot|LOT|-|--', 'null'))) <=4)
AND b.name311 
	NOT IN 
	(WITH filtered AS 
	(SELECT name311, COUNT(name311) AS namecount 
	FROM dpr_parksproperties_internal 
	GROUP BY name311) SELECT name311 FROM filtered WHERE namecount >= 2 )
	AND b.name311 NOT LIKE '%Bloomingdale%' AND a.description NOT LIKE '%&%'




-- IT agency feedback
-- Be a leader in IT and always use the most advanced technology we can
-- Open source
-- Embrace the cloud
-- If technology doesn't work great then that may result in low adoption (i.e. wifi is spotty)
-- Do more in house and depend less on DoITT since DoITT has a questionable track record
-- Embrace technology like a startup (young motivated staff expect a lot from technology)



-- construction distruption (low med high) *
-- direct visibility (binary) *
-- physical improvement (indirect)
-- 0 to 5 point scale

-- meeting with Kate Asher - ConEd live data not public but eager to share w/ city
-- dot.gov, borough commissioner plans is the data they use for paving data (not made public yet)

	---- notes from end of pgadmin
	--SELECT DISTINCT a.managingagency, a.projectid, target.maxdesc, target.description
--FROM omb_capitalcommitments_fy17_october16 a
--LEFT JOIN target ON length(replace(a.description, ' ', '')) = target.maxdesc AND a.projectid=target.projectid AND a.managingagency=target.managingagency
--GROUP BY a.managingagency, a.projectid, target.maxdesc, target.description
--ORDER BY a.managingagency, a.projectid, target.maxdesc, target.description DESC LIMIT 1 

--WITH target AS (SELECT managingagency, projectid, description, max(length(replace(description, ' ', ''))) AS longdesc
--    FROM omb_capitalcommitments_fy17_october16
--    GROUP BY managingagency, projectid, description)
--SELECT a.managingagency, a.projectid, a.description
--FROM omb_capitalcommitments_fy17_october16 a
--LEFT JOIN target b
--ON a.managingagency=b.managingagency AND a.projectid=b.projectid AND length(replace(a.description, ' ', '')) = b.longdesc
         
--GROUP BY managingagency, projectid, description


--SELECT DISTINCT managingagency||' '||projectid AS maprojectid, description 
--FROM omb_capitalcommitments_fy17_october16
--WHERE maprojectid IN (
--SELECT DISTINCT managingagency||' '||projectid AS maprojectid, 
--WHERE max(length(replace(description, ' ', ''))))
--WHERE 
--projectid = 'SE-851' AND managingagency = '850' AND

--length(replace(description, ' ', '')) = (SELECT max(length(replace(description, ' ', ''))) FROM omb_capitalcommitments_fy17_october16)

--SELECT managingagency, projectid, COUNT(*) FROM (
--SELECT DISTINCT managingagency, projectid, description 
--FROM omb_capitalcommitments_fy17_october16) a
--GROUP BY managingagency, projectid
--ORDER BY count DESC

-- "P-104HGBG", "P-412RWIL", 850 "SE-807", "SE848", "SE-851"

--SELECT DISTINCT managingagency, projectid, description, COUNT(*) FROM omb_capitalcommitments_fy17_october16 
--GROUP BY managingagency, projectid, description
--ORDER BY count desc


--SELECT 
--COUNT(*)
--DISTINCT domain, facilitygroup, facilitysubgroup, facilitytype, COUNT(*)
--FROM facilities
--WHERE geom IS NOT NULL AND borough <> 'Outside NYC'
--AND facilityname = 'Blue Heron Park Preserve'
--GROUP BY d=omain, facilitygroup, facilitysubgroup, facilitytype
--ORDER BY facilitytype, facilitysubgroup, facilitygroup,  DESC
--AND idagency IS NULL
--ORDER BY agencysourcey

--meeting notes 
--request: filter from all fields (i.e. level of government - federal, state)
-- use - acquisitions, bundeling of faclities
--sqft, zoning information
-- leased date / expiration data
--IPIS <-- touch base with DCAS IT to see how transfer data
--Fair share YAY
-- Travel shed - YES -
--Other filter - use RPAD to view vacant land - site search
-- email presentation 
-- ability to filter ZOLA better and other lot attributes for the use of identifying lots for facilities / co-locating
-- IPIS holds lots - capital planning database
--Randy and Matt - Joe Kaplin is IT
--Next step: Additional data points we put within this - make facility siting tool - what does that look like? What data do we need?

--asset maps - DEP assets 
-- DDC dependence 
-- shared project management systems (consultant) 
-- getting ideas on a map
-- DDC could be painpoint 


--meeting with FDNY
-- would like building materials in PLUTO
-- do not construction materials in city data
-- building base data intergration challenge --> MODA & Amen 
-- crims -- sids -- entered in through applicaton
-- building cards --> building footprint
-- buildings over 6 stories or more and expanding into hospitals
-- leveraging ArcGIS Online 
-- how are our datasets meeting our needs -- if there an effort in open data to make sure we're collecting what we need? - Kat director
-- varies by incident - what facilities are used during emergencies?
-- up to date info on clinic locations
-- FDNY - overused and ineffiecient 
-- hours of operation
-- EM - hazard mitigation tool
-- cda inspections
-- what does a batallion profile look like - FDNY profile
-- give access to beta testers 
-- building based informaiton - connect with Peter at populaiton

--meeting with DSNY and CUSP
-- Local Law 84 data has number of bedrooms for big buildings
-- DEP H2O data
-- Consistine send Joe what they're working on with 311 complaints
-- give Dan and CUSP access to Platform
-- second meeting - CUSP, PLUTO	 --> Yuan

--meeting with DOT 10/08
--managing and tracking capital projects
-- product focused on CPI and CEP needs 
-- SQL database
-- mapbox for open source
-- working on workflow management
-- database of assets and workdlow
-- can provide: will expose web services so we can tap into API
-- push back changes to DOT and other agencies
-- start with data consumption
-- NYCStreets - DOT / DEP / DDC mapping system for street work
-- Data gap between DDC and DOT - 
-- quartely meetings with NYCmap dev team

--Meeting with Carl
-- Working through data governance
-- Pernimia - issues on data quality, wants to know when the data were last updated
-- Carl questions.  Carl: governance?  update frequency?  access? 
-- Questions to answer before OMB - Data governance, maintence and quality control, access issue - who sees what, when?
-- How should those three issues be addressed?? ^^


-- OEM underground infrastractue meeting
-- Data security
-- LRS
-- Soil, pipes, water

-- Meeting with Hannah and Chris
-- of total projects
-- explain what terms are underneath bar chart --> can be web page
-- Treating like it a campaign
-- priortize agencies
-- How do we do distributed dataset editing? --> elephant in the room
-- Knock this down to a reasonable dataset in house --> send to agencies to verify data --> start conversation that way of is this right and how does it fit into your planning process? --> all web based 

-- meeting with Amen
-- Singapore - travel shed demo
-- email - invite to github / waffle - use cases
-- MIT - database intellegence tool
-- DOF & DOHMH have data governance issues 






