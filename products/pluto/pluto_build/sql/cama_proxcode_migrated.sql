-- assigning proxy code
-- recode DOF values to DCP values and remove 0s (Not Available)
-- select proxcode for lot from record where bldgnum is 1
-- all bbl reocrds have at least one record with a bldgnum = '1'
WITH dcpcamavals AS (
    SELECT
        primebbl AS bbl,
        (CASE
            WHEN proxcode = '5' THEN '2'
            WHEN proxcode = '4' THEN '3'
            WHEN proxcode = '6' THEN '3'
            ELSE proxcode
        END) AS proxcode
    FROM pluto_input_cama
    WHERE
        proxcode != '0'
        AND proxcode != 'N'
        AND bldgnum = '1'
),

max_bbl_proxcodes AS (
    SELECT
        bbl,
        max(proxcode) AS proxcode
    FROM dcpcamavals
    GROUP BY bbl
)

UPDATE pluto a
SET proxcode = m.proxcode
FROM max_bbl_proxcodes AS m
WHERE a.bbl = m.bbl;

-- assign 0 (Not Available) to remaining records
UPDATE pluto
SET proxcode = '0'
WHERE proxcode IS NULL;
