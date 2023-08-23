-- INPUTS ALL RECORDS SEPERATED BY A ; 
-- -- if the lot is in a historical district add in the name of the Historic District from lpc_historic_districts
-- -- concatenates historic districts seperated by a semi colon for lots within more than one Historic District 
-- UPDATE pluto a
-- SET histdist = c.hist_dist_list
-- FROM (SELECT b.bbl, string_agg(b.hist_dist, '; ') AS hist_dist_list
-- 		FROM  (
-- 			SELECT DISTINCT bbl, hist_dist 
-- 			FROM lpc_historic_districts
-- 			ORDER BY hist_dist) AS b
-- 	WHERE hist_dist <> '0'
-- 	GROUP BY b.bbl) c
-- WHERE a.borocode||lpad(a.block, 5, '0')||lpad(a.lot, 4, '0') = c.bbl;

-- -- if the lot contains a landmark add in the name of the landmark from lpc_landmarks
-- -- concatenates landmark names seperated by a semi colon for lots with more than one landmark
-- UPDATE pluto a
-- SET landmark = c.lm_name_list
-- FROM (SELECT b.bbl, string_agg(b.lm_name, '; ') AS lm_name_list
-- 		FROM (
-- 			SELECT DISTINCT bbl, lm_name 
-- 			FROM lpc_landmarks
-- 			ORDER BY lm_name) AS b
-- 		GROUP BY b.bbl) c
-- WHERE a.borocode||lpad(a.block, 5, '0')||lpad(a.lot, 4, '0') = c.bbl;

-- INPUTS FIRST ALPHABETICAL RECORD
-- if the lot is in a historical district add in the name of the Historic District from lpc_historic_districts
-- the first alphabetical historical district is appended
WITH histdistricts AS (
	SELECT bbl, hist_dist 
	FROM (
		SELECT bbl, hist_dist, ROW_NUMBER()
    	OVER (PARTITION BY bbl
      	ORDER BY hist_dist) AS row_number
  		FROM lpc_historic_districts
  		WHERE hist_dist <> '0'
  		AND hist_dist <> 'Individual Landmarks') x
	WHERE x.row_number = 1)

UPDATE pluto a
SET histdist = histdistricts.hist_dist
FROM histdistricts
WHERE a.borocode||lpad(a.block, 5, '0')||lpad(a.lot, 4, '0') = histdistricts.bbl;

-- if the lot contains a landmark add mark it as an Interior, Individual, or Individual and Interior Landmark
WITH landmarks AS (
  SELECT DISTINCT bbl,lm_type, ROW_NUMBER()
      OVER (PARTITION BY bbl
        ORDER BY lm_type) AS row_number 
      FROM (
            SELECT DISTINCT bbl, lm_type 
            FROM lpc_landmarks 
            WHERE (lm_type = 'Interior Landmark' OR lm_type = 'Individual Landmark')
            AND status = 'DESIGNATED'
            AND most_curre = '1'
            AND (last_actio = 'DESIGNATED' OR last_actio = 'DESIGNATED (AMENDMENT/MODIFICATION ACCEPTED)')
            ) x ),
maxnum AS (
  SELECT bbl, max(row_number) as maxrow_number FROM landmarks GROUP BY bbl)

UPDATE pluto a
SET landmark = (CASE
                WHEN c.maxrow_number = 1 THEN upper(b.lm_type)
                WHEN c.maxrow_number = 2 THEN upper('Individual and Interior Landmark')
                ELSE upper(b.lm_type)
                END)
FROM landmarks b, maxnum c
WHERE a.borocode||lpad(a.block, 5, '0')||lpad(a.lot, 4, '0') = b.bbl
AND c.bbl = b.bbl;