-- assigning basement type
-- remove 0 (Unknown) bsmnt_type 
-- get highest bsmnt_type and bsmntgradient value for each lot
-- match bsmnt_type and bsmntgradient values to pluto_input_bsmtcode lookup table to get and assign bsmtcode value
WITH dcpcamavals AS(
	SELECT DISTINCT x.bbl, x.bsmnt_type, x.bsmntgradient, b.bsmtcode
	FROM (
		SELECT primebbl AS bbl, bsmnt_type, bsmntgradient, ROW_NUMBER()
    	OVER (PARTITION BY LEFT(bbl,10)
      	ORDER BY bsmnt_type DESC, bsmntgradient DESC) AS row_number
  		FROM pluto_input_cama
  		WHERE bsmnt_type <> '0'
		AND bldgnum = '1') x
	LEFT JOIN pluto_input_bsmtcode b
	ON x.bsmnt_type = b.bsmnt_type AND x.bsmntgradient = b.bsmntgradient
	WHERE x.row_number = 1)

UPDATE pluto a
SET bsmtcode = b.bsmtcode
FROM dcpcamavals b
WHERE a.bbl=b.bbl;

-- assign 5 (Unknown) to remaining records
UPDATE pluto
SET bsmtcode = '5'
WHERE bsmtcode IS NULL;