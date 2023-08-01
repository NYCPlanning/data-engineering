-- assigning lot type
-- remove 0s (Not Available) and 5 (none of the other types)
-- select lowest lot type value for record where bldgnum is 1
WITH dcpcamavals AS(
	SELECT DISTINCT bbl,
	lottype
	FROM (
		SELECT primebbl AS bbl, lottype, ROW_NUMBER()
    	OVER (PARTITION BY LEFT(bbl,10)
      	ORDER BY lottype) AS row_number
  		FROM pluto_input_cama
  		WHERE lottype <> '0' 
		AND lottype <> '5'
		AND bldgnum = '1') x
	WHERE x.row_number = 1)	

UPDATE pluto a
SET lottype = dcpcamavals.lottype
FROM dcpcamavals
WHERE a.bbl = dcpcamavals.bbl;

-- update lots with lot type value of 5
UPDATE pluto a
SET lottype = b.lottype
FROM pluto_input_cama b
WHERE a.bbl = b.primebbl
AND b.lottype = '5'
AND a.lottype IS NULL;

-- assign 0 (Mixed or Unknown) to remaining records
UPDATE pluto
SET lottype = '0'
WHERE lottype IS NULL;