-- set the number of easements associated with a lot
-- get a list of distinct easements for each lot
WITH distincteasements AS (
	SELECT DISTINCT primebbl as bbl,
	ease
	FROM dof_pts_propmaster b
	WHERE ease IS NOT NULL AND ease <> ' '
),
-- count the number of distinct easements for a lot
counteasements AS (
	SELECT bbl,
	COUNT(*) as numeasements
	FROM distincteasements b
	WHERE b.ease IS NOT NULL
	GROUP BY b.bbl
)
-- set the number of easements for the lot
UPDATE pluto a
SET easements = b.numeasements
FROM counteasements b
WHERE a.bbl = b.bbl;
-- for recrods whith no easement set number of easements to 0
UPDATE pluto
SET easements = '0'
WHERE easements IS NULL;

-- using RPAD as source
-- DROP TABLE IF EXISTS pluto_input_rpad;
-- CREATE TABLE pluto_input_rpad AS (
-- SELECT a.*, b.billingbbl
-- FROM pluto_rpad a
-- LEFT JOIN pluto_input_geocodes b
-- ON a.boro||a.tb||a.tl=b.borough||lpad(b.block,5,'0')||lpad(b.lot,4,'0')
-- );

-- ALTER TABLE pluto_input_rpad
-- ADD COLUMN primebbl text;

-- UPDATE pluto_input_rpad
-- SET primebbl = billingbbl
-- WHERE billingbbl IS NOT NULL AND billingbbl <> '0000000000';
-- -- assign prime bbl for noncondo lots
-- UPDATE pluto_input_rpad
-- SET primebbl = boro||tb||tl
-- WHERE primebbl IS NULL;

-- WITH distincteasements AS (
-- 	SELECT DISTINCT primebbl as bbl,
-- 	ease
-- 	FROM pluto_input_rpad b
-- 	WHERE ease IS NOT NULL AND ease <> ' '
-- ),
-- -- count the number of distinct easements for a lot
-- counteasements AS (
-- 	SELECT bbl,
-- 	COUNT(*) as numeasements
-- 	FROM distincteasements b
-- 	WHERE b.ease IS NOT NULL
-- 	GROUP BY b.bbl
-- )
-- -- set the number of easements for the lot
-- UPDATE pluto a
-- SET easements = b.numeasements
-- FROM counteasements b
-- WHERE a.bbl = b.bbl;