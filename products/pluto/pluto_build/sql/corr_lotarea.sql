-- Where the lot area is zero but the geometry is not null
-- compute the lot area from the geometry in sq feet

-- Insert old and new values into tracking table
INSERT INTO pluto_changes_applied
SELECT DISTINCT
    a.bbl,
    'lotarea' AS field,
    a.lotarea AS old_value,
    round(ST_Area(ST_Transform(a.geom, 2263)))::text AS new_value,
    '1' AS type,
    'Zero lot area' AS reason,
    a.version
FROM pluto AS a
WHERE
    a.lotarea = '0'
    AND a.geom IS NOT NULL
    AND a.bbl NOT IN (
        SELECT bbl FROM pluto_input_research
        WHERE field = 'lotarea'
    );

-- -- Redundant code	
-- -- Apply correction
-- UPDATE pluto a
-- SET lotarea = b.new_value,
-- 	dcpedited = 't'
-- FROM pluto_corrections b
-- WHERE a.bbl = b.bbl
-- 	AND b.field = 'lotarea'
-- 	AND a.lotarea = '0' 
-- 	AND a.geom IS NOT NULL;

-- Take researched lot area values from the research table
INSERT INTO pluto_changes_not_applied
SELECT DISTINCT
    b.*,
    a.lotarea AS found_value
FROM pluto_input_research AS b, pluto AS a
WHERE
    b.bbl = a.bbl
    AND b.field = 'lotarea'
    AND b.old_value::numeric != a.lotarea::numeric;

INSERT INTO pluto_changes_applied
SELECT DISTINCT b.*
FROM pluto_input_research AS b, pluto AS a
WHERE
    b.bbl = a.bbl
    AND b.field = 'lotarea'
    AND b.old_value::numeric = a.lotarea::numeric;

-- Apply correction to PLUTO
UPDATE pluto a
SET
    lotarea = b.new_value,
    dcpedited = 't'
FROM pluto_changes_applied AS b
WHERE
    a.bbl = b.bbl
    AND b.field = 'lotarea'
    AND a.lotarea::numeric = b.old_value::numeric;

-- Take researched building area values from the research table
INSERT INTO pluto_changes_not_applied
SELECT DISTINCT
    b.*,
    a.bldgarea AS found_value
FROM pluto_input_research AS b, pluto AS a
WHERE
    b.bbl = a.bbl
    AND b.field = 'bldgarea'
    AND b.old_value::numeric != a.bldgarea::numeric;

INSERT INTO pluto_changes_applied
SELECT DISTINCT b.*
FROM pluto_input_research AS b, pluto AS a
WHERE
    b.bbl = a.bbl
    AND b.field = 'bldgarea'
    AND b.old_value::numeric = a.bldgarea::numeric;

-- Apply correction to PLUTO
UPDATE pluto a
SET
    bldgarea = b.new_value,
    dcpedited = 't'
FROM pluto_input_research AS b
WHERE
    a.bbl = b.bbl
    AND b.field = 'bldgarea'
    AND a.bldgarea::numeric = b.old_value::numeric;

-- recalculate builtfar
UPDATE pluto
SET builtfar = round(bldgarea::numeric / lotarea::numeric, 2)
WHERE
    lotarea != '0'
    AND lotarea IS NOT NULL
    AND bbl IN (
        SELECT bbl FROM pluto_changes_applied
        WHERE field = 'lotarea' OR field = 'bldgarea'
    );


-- lot frontage
INSERT INTO pluto_changes_not_applied
SELECT DISTINCT
    b.*,
    a.lotfront AS found_value
FROM pluto_input_research AS b, pluto AS a
WHERE
    b.bbl = a.bbl
    AND b.field = 'lotfront'
    AND b.old_value::numeric != a.lotfront::numeric;

INSERT INTO pluto_changes_applied
SELECT DISTINCT b.*
FROM pluto_input_research AS b, pluto AS a
WHERE
    b.bbl = a.bbl
    AND b.field = 'lotfront'
    AND b.old_value::numeric = a.lotfront::numeric;

-- Apply correction to PLUTO
UPDATE pluto a
SET
    lotfront = b.new_value,
    dcpedited = 't'
FROM pluto_changes_applied AS b
WHERE
    a.bbl = b.bbl
    AND b.field = 'lotfront'
    AND a.lotfront::numeric = b.old_value::numeric;


-- lot depth
INSERT INTO pluto_changes_not_applied
SELECT DISTINCT
    b.*,
    a.lotdepth AS found_value
FROM pluto_input_research AS b, pluto AS a
WHERE
    b.bbl = a.bbl
    AND b.field = 'lotdepth'
    AND b.old_value::numeric != a.lotdepth::numeric;

INSERT INTO pluto_changes_applied
SELECT DISTINCT b.*
FROM pluto_input_research AS b, pluto AS a
WHERE
    b.bbl = a.bbl
    AND b.field = 'lotdepth'
    AND b.old_value::numeric = a.lotdepth::numeric;

-- Apply correction to PLUTO
UPDATE pluto a
SET
    lotdepth = b.new_value,
    dcpedited = 't'
FROM pluto_changes_applied AS b
WHERE
    a.bbl = b.bbl
    AND b.field = 'lotdepth'
    AND a.lotdepth::numeric = b.old_value::numeric;
