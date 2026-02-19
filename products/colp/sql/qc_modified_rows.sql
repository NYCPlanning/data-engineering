DROP VIEW IF EXISTS qc_modified_rows_changed_uses;
CREATE VIEW qc_modified_rows_changed_uses AS
WITH new AS (
    SELECT
        "BBL" AS bbl,
        "ADDRESS" AS address,
        "AGENCY" AS agency,
        ARRAY_AGG(DISTINCT "USETYPE") AS uses
    FROM colp
    GROUP BY "BBL", "ADDRESS", "AGENCY"
), old AS (
    SELECT
        bbl::numeric::bigint,
        address,
        agency,
        ARRAY_AGG(DISTINCT usetype::text) AS uses
    FROM dcp_colp
    GROUP BY bbl, address, agency
), all_diffs AS (
    SELECT
        new.bbl,
        new.address,
        new.agency,
        old.uses AS old_uses,
        new.uses AS new_uses
    FROM new
    INNER JOIN old ON
        new.bbl = old.bbl
        AND new.address = old.address
        AND new.agency = old.agency
        AND new.uses <> old.uses
)
SELECT
    old_uses[1] AS old_use,
    new_uses[1] AS new_use,
    COUNT(*) AS cnt
    -- ARRAY_AGG((bbl, address, agency)) AS locs
FROM all_diffs
WHERE ARRAY_LENGTH(new_uses, 1) = 1 AND ARRAY_LENGTH(old_uses, 1) = 1
GROUP BY old_uses[1], new_uses[1]
ORDER BY COUNT(*) DESC;

DROP VIEW IF EXISTS qc_modified_rows_removed;
CREATE VIEW qc_modified_rows_removed AS
WITH new AS (
    SELECT
        "BBL" AS bbl,
        "ADDRESS" AS address,
        "AGENCY" AS agency,
        ARRAY_AGG(DISTINCT "USETYPE") AS uses
    FROM colp
    GROUP BY "BBL", "ADDRESS", "AGENCY"
), old AS (
    SELECT
        bbl::numeric::bigint,
        address,
        agency,
        parcelname,
        ARRAY_AGG(DISTINCT usetype::text) AS uses
    FROM dcp_colp
    GROUP BY bbl, address, agency, parcelname
), all_diffs AS (
    SELECT
        old.bbl,
        old.address,
        old.parcelname,
        old.agency,
        old.uses AS old_uses,
        new.uses AS new_uses
    FROM old LEFT JOIN new
        ON old.bbl = new.bbl AND old.address = new.address AND old.agency = new.agency
--WHERE 'HIGHER EDUCATION' = ANY(old.uses)
)
SELECT *
FROM all_diffs;

DROP VIEW IF EXISTS qc_modified_rows_new;
CREATE VIEW qc_modified_rows_new AS
WITH new AS (
    SELECT
        "BBL" AS bbl,
        "ADDRESS" AS address,
        "AGENCY" AS agency,
        "PARCELNAME" AS parcelname,
        ARRAY_AGG(DISTINCT "USETYPE") AS uses
    FROM colp
    GROUP BY "BBL", "ADDRESS", "AGENCY", "PARCELNAME"
), old AS (
    SELECT
        bbl::numeric::bigint,
        address,
        agency,
        parcelname,
        ARRAY_AGG(DISTINCT usetype::text) AS uses
    FROM dcp_colp
    GROUP BY bbl, address, agency, parcelname
), all_diffs AS (
    SELECT
        new.bbl,
        new.address,
        new.parcelname,
        new.agency,
        old.uses AS old_uses,
        new.uses AS new_uses
    FROM new LEFT JOIN old
        ON new.bbl = old.bbl AND new.address = old.address AND new.agency = old.agency
--WHERE 'HIGHER EDUCATION' = ANY(new.uses)
)
SELECT *
FROM all_diffs;
