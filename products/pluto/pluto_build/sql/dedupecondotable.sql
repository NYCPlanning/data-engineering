--copy condo table from DOF
DROP TABLE IF EXISTS pluto_condo;
CREATE TABLE pluto_condo AS (
    SELECT * FROM dof_condo
);
ALTER TABLE pluto_condo ADD COLUMN id SERIAL PRIMARY KEY;

-- remove duplicate records
DELETE FROM pluto_condo
WHERE id IN (
    SELECT id
    FROM (
        SELECT
            id,
            ROW_NUMBER() OVER (
                PARTITION BY condo_base_bbl, condo_billing_bbl
                ORDER BY condo_base_bbl, condo_billing_bbl
            ) AS row_num
        FROM pluto_condo
    ) AS t
    WHERE t.row_num > 1
);

-- remove duplicate records where billing bbl is NULL
DELETE FROM pluto_condo
WHERE id IN (
    SELECT id
    FROM (
        SELECT
            id,
            ROW_NUMBER() OVER (
                PARTITION BY condo_base_bbl
                ORDER BY condo_base_bbl
            ) AS row_num
        FROM pluto_condo
    ) AS t
    WHERE t.row_num > 1
)
AND condo_billing_bbl IS NULL;

-- remove duplicate records where base bbl is the same but billing bbl is different
DELETE FROM pluto_condo
WHERE id IN (
    SELECT id
    FROM (
        SELECT
            id,
            ROW_NUMBER() OVER (
                PARTITION BY condo_base_bbl
                -- TODO This hard-coding is a temporary fix to preserve ordering from 23v3
                -- 24v1 should have the `condo_billing_bbl IN ...` removed, and order just by condo_billing_bbl
                ORDER BY condo_billing_bbl IN (
                    '3002787502',
                    '3003047501',
                    '4050667502',
                    '4050667507',
                    '4076217502',
                    '5014957501',
                    '5024017501',
                    '5030247501',
                    '5031737504',
                    '5070487502',
                    '5070557501'
                ) DESC,
                condo_billing_bbl
            ) AS row_num
        FROM pluto_condo
        WHERE condo_billing_bbl IS NOT NULL
    ) AS t
    WHERE t.row_num > 1
);
