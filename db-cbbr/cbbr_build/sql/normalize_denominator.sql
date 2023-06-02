ALTER TABLE _cbbr_submissions
    DROP COLUMN IF EXISTS denominator;

ALTER TABLE _cbbr_submissions
    ADD COLUMN denominator text;

WITH denominatorcount AS (
    SELECT
        commdist,
        type_br,
        COUNT(*) AS count
    FROM
        _cbbr_submissions
    GROUP BY
        commdist,
        type_br)
UPDATE
    _cbbr_submissions a
SET
    denominator = b.count
FROM
    denominatorcount b
WHERE
    a.commdist = b.commdist
    AND a.type_br = b.type_br;

