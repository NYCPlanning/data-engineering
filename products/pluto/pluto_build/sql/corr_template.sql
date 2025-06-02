INSERT INTO pluto_changes_not_applied
SELECT DISTINCT
    b.*,
    a.:FIELD AS found_value
FROM pluto_input_research AS b, pluto AS a
WHERE
    b.bbl = a.bbl
    AND b.field = :'FIELD'
    AND a.:FIELD IS DISTINCT FROM b.old_value;

INSERT INTO pluto_changes_applied
SELECT DISTINCT b.*
FROM pluto_input_research AS b, pluto AS a
WHERE
    b.bbl = a.bbl
    AND b.field = :'FIELD'
    AND a.:FIELD IS NOT DISTINCT FROM b.old_value;

-- Apply correction to PLUTO
UPDATE pluto a
SET
    :FIELD = b.new_value,
    dcpedited = 't'
FROM pluto_input_research AS b
WHERE
    a.bbl = b.bbl
    AND b.field = :'FIELD'
    AND a.:FIELD IS NOT DISTINCT FROM b.old_value;
