-- Take the year built from researched lpc_historic_districts table

INSERT INTO pluto_changes_not_applied
SELECT DISTINCT
    b.*,
    a.bct2020 AS found_value
FROM pluto_input_research AS b, pluto AS a
WHERE
    b.bbl = a.bbl
    AND b.field = 'bct2020'
    AND b.old_value != a.bct2020;

INSERT INTO pluto_changes_applied
SELECT DISTINCT b.*
FROM pluto_input_research AS b, pluto AS a
WHERE
    b.bbl = a.bbl
    AND b.field = 'bct2020'
    AND b.old_value = a.bct2020;

-- Apply correction to PLUTO
UPDATE pluto a
SET
    bct2020 = b.new_value,
    dcpedited = 't'
FROM pluto_input_research AS b
WHERE
    a.bbl = b.bbl
    AND b.field = 'bct2020'
    AND a.bct2020 = b.old_value;
