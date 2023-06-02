DROP TABLE IF EXISTS pluto_input_cama;
CREATE TABLE pluto_input_cama AS (
SELECT a.*, b.billingbbl
FROM pluto_input_cama_dof a
LEFT JOIN pluto_input_geocodes b
ON LEFT(a.bbl,10)=b.borough||lpad(b.block,5,'0')||lpad(b.lot,4,'0')
);

-- test logic to mimic pts logic
-- DROP TABLE IF EXISTS pluto_input_cama;
-- CREATE TABLE pluto_input_cama AS (
-- WITH pluto_cama_rownum AS (
-- 	SELECT a.*, ROW_NUMBER()
--     	OVER (PARTITION BY LEFT(a.bbl,10)
--       	ORDER BY grossarea::numeric DESC, officearea::numeric DESC, residarea::numeric DESC) AS row_number
--   		FROM pluto_input_cama_dof a),
-- pluto_cama_sub AS (
-- 	SELECT * 
-- 	FROM pluto_cama_rownum 
-- 	WHERE row_number = 1)
-- SELECT a.*, b.billingbbl
-- FROM pluto_cama_sub a
-- LEFT JOIN pluto_input_geocodes b
-- ON LEFT(a.bbl,10)=b.borough||lpad(b.block,5,'0')||lpad(b.lot,4,'0')
-- );

ALTER TABLE pluto_input_cama
ADD COLUMN primebbl text;

UPDATE pluto_input_cama
SET primebbl = billingbbl
WHERE billingbbl IS NOT NULL AND billingbbl <> '0000000000';
-- assign prime bbl for noncondo lots
UPDATE pluto_input_cama
SET primebbl = LEFT(bbl,10)
WHERE primebbl IS NULL;