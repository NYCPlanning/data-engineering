-- create summary table reporting how many projects and total $ of planned commitments agencies added to the map or corrected
DROP TABLE IF EXISTS agency_validated_geoms_summary_table;
CREATE TABLE agency_validated_geoms_summary_table AS(
WITH newgeoms AS (
	SELECT a.agency,
	COUNT(a.*) AS countnew,
	SUM(b.totalcost) AS totalplannedcommitnew
	FROM dcp_cpdb_agencyverified a
	LEFT JOIN cpdb_projects b
	ON a.maprojid=b.maprojid
	WHERE origin = 'unmapped'
	AND a.geom IS NOT NULL
	GROUP BY agency
),
correctedgeoms AS (
	SELECT a.agency,
	COUNT(a.*) AS countcorrected,
	SUM(b.totalcost) AS totalplannedcommitcorrected
	FROM dcp_cpdb_agencyverified a
	LEFT JOIN cpdb_projects b
	ON a.maprojid=b.maprojid
	WHERE origin = 'mapped'
	AND a.geom IS NOT NULL
	GROUP BY agency
),
removedgeoms AS (
	SELECT a.agency,
	COUNT(a.*) AS countremoved,
	SUM(b.totalcost) AS totalplannedcommitremoved
	FROM dcp_cpdb_agencyverified a
	LEFT JOIN cpdb_projects b
	ON a.maprojid=b.maprojid
	WHERE origin = 'mapped'
	AND (mappable = 'No - Can be in future'
OR mappable = 'No - Can never be mapped')
	GROUP BY agency
),
totalmappedrecords AS (
	SELECT a.agency,
	COUNT(a.*) AS totalmapped
	FROM dcp_cpdb_agencyverified a
	WHERE origin = 'mapped'
	GROUP BY agency
),
totalunmappedrecords AS (
	SELECT a.agency,
	COUNT(a.*) AS totalunmapped
	FROM dcp_cpdb_agencyverified a
	WHERE origin = 'unmapped'
	GROUP BY agency
)

SELECT a.agency, 
totalmapped, 
totalunmapped, 
countcorrected, 
totalplannedcommitcorrected,
countnew, 
totalplannedcommitnew,
countremoved,
totalplannedcommitremoved,
(countcorrected+countnew)-countremoved AS countnetchange,
(totalplannedcommitcorrected+totalplannedcommitnew) - totalplannedcommitremoved AS totalplannedcommitnetchange
FROM totalunmappedrecords a
LEFT JOIN totalmappedrecords b
ON a.agency=b.agency
LEFT JOIN correctedgeoms c
ON a.agency=c.agency
LEFT JOIN newgeoms d
ON a.agency= d.agency
LEFT JOIN removedgeoms e
ON a.agency= e.agency
ORDER BY a.agency
);