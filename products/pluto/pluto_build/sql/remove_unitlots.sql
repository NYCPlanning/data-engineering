-- drop records from pluto

-- create table to insert dropped records
DROP TABLE IF EXISTS pluto_removed_records;
CREATE TABLE pluto_removed_records (LIKE pluto);

-- where it's a unit lot, it's unmapped, and the number of units is > 1
-- unit lots have lots >= 1000, though 75xx is reserved for condo billing bbls
INSERT INTO pluto_removed_records (
    SELECT * FROM pluto
    WHERE
        lot::numeric >= 1000
        AND lot NOT LIKE '75%'
        AND unitstotal::numeric > 1
        AND geom IS NULL
);

DELETE FROM pluto
WHERE
    lot::numeric >= 1000
    AND lot NOT LIKE '75%'
    AND unitstotal::numeric > 1
    AND geom IS NULL;

-- where it's a unit lot and the address is the same andthe building area is the same
INSERT INTO pluto_removed_records (
    WITH thousandlots AS (
        SELECT * FROM pluto
        WHERE lot::numeric >= 1000 AND lot NOT LIKE '75%'
    ),

    overcouting AS (
        SELECT
            address,
            bldgarea,
            count(*) AS count
        FROM thousandlots
        WHERE
            address IS NOT NULL
            AND bldgarea::numeric > 0
        GROUP BY address, bldgarea
    ),

    tobedropped AS (
        SELECT * FROM overcouting
        WHERE count > 1
    )

    SELECT * FROM pluto
    WHERE address || bldgarea IN (
        SELECT address || bldgarea FROM tobedropped
    )
    AND lot::numeric >= 1000 AND lot NOT LIKE '75%'
);

DELETE FROM pluto
WHERE bbl IN (SELECT bbl FROM pluto_removed_records);
