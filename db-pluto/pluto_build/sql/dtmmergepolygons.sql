--copy condo table from source DOF
DROP TABLE IF EXISTS pluto_dtm;
CREATE TABLE pluto_dtm AS(
SELECT * FROM dof_dtm);
ALTER TABLE pluto_dtm ADD COLUMN primebbl text;

-- Set the prime bbl as the billing bbl for condo lots
-- using the pluto condo table
UPDATE pluto_dtm
SET primebbl = condo_billing_bbl
FROM pluto_condo
WHERE bbl=condo_base_bbl
AND condo_billing_bbl IS NOT NULL;

-- create merged geometries for condo records
DROP TABLE IF EXISTS pluto_dtm_condosmerged;
CREATE TABLE pluto_dtm_condosmerged AS (
SELECT primebbl, ST_Union(geom) geom
FROM pluto_dtm
WHERE primebbl IS NOT NULL
GROUP BY primebbl);
-- create merged geometries for non-condo records
DROP TABLE IF EXISTS pluto_dtm_noncondosmerged;
CREATE TABLE pluto_dtm_noncondosmerged AS (
SELECT bbl, ST_Union(ST_MakeValid(geom)) geom
FROM pluto_dtm
WHERE bbl IS NOT NULL
AND primebbl IS NULL
-- AND st_isvalidreason(geom) <> 'IllegalArgumentException: Invalid number of points in LinearRing found 3 - must be 0 or >= 4'
GROUP BY bbl);
-- merge condo and non condo records into one table
DROP TABLE IF EXISTS pluto_dtm;
CREATE TABLE pluto_dtm AS(
	SELECT primebbl as bbl, 'Y' as condo, geom
	FROM pluto_dtm_condosmerged
	UNION ALL
	SELECT bbl, NULL as condo, geom
	FROM pluto_dtm_noncondosmerged);
-- merge condo and non condo records into one table
DROP TABLE IF EXISTS pluto_dtm_condosmerged;
DROP TABLE IF EXISTS pluto_dtm_noncondosmerged;