--copy condo table from DOF
DROP TABLE IF EXISTS pluto_condo;
CREATE TABLE pluto_condo AS(
SELECT * FROM dof_condo);
ALTER TABLE pluto_condo ADD COLUMN id SERIAL PRIMARY KEY;

-- remove duplicate records
DELETE FROM pluto_condo
WHERE id IN (
    SELECT id
    FROM (
        SELECT id,
        ROW_NUMBER() OVER( PARTITION BY condo_base_bbl,condo_billing_bbl
        ORDER BY  condo_base_bbl,condo_billing_bbl ) AS row_num
        FROM pluto_condo ) t
        WHERE t.row_num > 1 );

-- remove duplicate records where billing bbl is NULL
DELETE FROM pluto_condo
WHERE id IN (
    SELECT id 
    FROM (
        SELECT id,
        ROW_NUMBER() OVER(PARTITION BY condo_base_bbl
        ORDER BY condo_base_bbl) AS row_num
        FROM pluto_condo) t
        WHERE t.row_num > 1)
AND condo_billing_bbl IS NULL;

-- remove duplicate records where base bbl is the same but billing bbl is different
DELETE FROM pluto_condo
WHERE id IN (
    SELECT id 
    FROM (
        SELECT id,
        ROW_NUMBER() OVER(PARTITION BY condo_base_bbl
        ORDER BY condo_base_bbl) AS row_num
        FROM pluto_condo) t
        WHERE t.row_num > 1)
AND condo_billing_bbl NOT IN (SELECT DISTINCT billingbbl 
                        FROM pluto_rpad_geo
                        WHERE billingbbl IS NOT NULL);