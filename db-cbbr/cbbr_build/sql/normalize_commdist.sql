ALTER TABLE _cbbr_submissions
    DROP COLUMN IF EXISTS commdist;

ALTER TABLE _cbbr_submissions
    ADD COLUMN borough_code text;

ALTER TABLE _cbbr_submissions
    ADD COLUMN commdist text;

-- OPTION A: Use provided FY2024 values
-- UPDATE
--     _cbbr_submissions
-- SET
--     commdist = boro_and_board;
-- OPTION B: Construct values to align with previous CBBR versions
UPDATE
    _cbbr_submissions a
SET
    borough_code = '1',
    commdist = '1' || lpad(cd, 2, '0')
WHERE
    a.borough = 'Manhattan'
    AND commdist IS NULL;

UPDATE
    _cbbr_submissions a
SET
    borough_code = '2',
    commdist = '2' || lpad(cd, 2, '0')
WHERE
    a.borough = 'Bronx'
    AND commdist IS NULL;

UPDATE
    _cbbr_submissions a
SET
    borough_code = '3',
    commdist = '3' || lpad(cd, 2, '0')
WHERE
    a.borough = 'Brooklyn'
    AND commdist IS NULL;

UPDATE
    _cbbr_submissions a
SET
    borough_code = '4',
    commdist = '4' || lpad(cd, 2, '0')
WHERE
    a.borough = 'Queens'
    AND commdist IS NULL;

UPDATE
    _cbbr_submissions a
SET
    borough_code = '5',
    commdist = '5' || lpad(cd, 2, '0')
WHERE
    a.borough = 'SI'
    AND commdist IS NULL;

