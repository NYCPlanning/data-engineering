-- Take the unitstotal value from research table
-- Insert records into pluto_corrections
-- INSERT INTO pluto_corrections
-- SELECT DISTINCT a.bbl, 
-- 	'unitstotal' as field, 
-- 	a.unitstotal as old_value, 
-- 	b.new_value as new_value,
-- 	b.type as type,
-- 	b.reason as reason,
-- 	b.version as version
-- FROM pluto a, pluto_input_research b
-- WHERE a.bbl = b.bbl
-- 	AND a.unitstotal=b.old_value
-- 	AND b.field = 'unitstotal'
-- 	AND a.bbl NOT IN (SELECT bbl FROM pluto_corrections WHERE field = 'unitstotal');

INSERT INTO pluto_changes_not_applied
SELECT DISTINCT b.*
FROM pluto_input_research b, pluto a
WHERE b.bbl=a.bbl 
	AND b.field='unitstotal' 
	AND b.old_value <> a.unitstotal;

INSERT INTO pluto_changes_applied
SELECT DISTINCT b.*
FROM pluto_input_research b, pluto a
WHERE b.bbl=a.bbl 
	AND b.field='unitstotal' 
	AND b.old_value = a.unitstotal;

-- Apply correction to PLUTO
UPDATE pluto a
SET unitstotal = b.new_value,
	dcpedited = 't'
FROM pluto_input_research b
WHERE a.bbl = b.bbl
	AND b.field = 'unitstotal'
	AND a.unitstotal=b.old_value;

-- Take the unitsres value from research table
-- Insert records into pluto_corrections
-- INSERT INTO pluto_corrections
-- SELECT DISTINCT a.bbl, 
-- 	'unitsres' as field, 
-- 	a.unitsres as old_value, 
-- 	b.new_value as new_value,
-- 	b.type as type,
-- 	b.reason as reason,
-- 	b.version as version
-- FROM pluto a, pluto_input_research b
-- WHERE a.bbl = b.bbl
-- 	AND a.unitsres=b.old_value
-- 	AND b.field = 'unitsres'
-- 	AND a.bbl NOT IN (SELECT bbl FROM pluto_corrections WHERE field = 'unitsres');

INSERT INTO pluto_changes_not_applied
SELECT DISTINCT b.*
FROM pluto_input_research b, pluto a
WHERE b.bbl=a.bbl 
	AND b.field='unitsres' 
	AND b.old_value <> a.unitsres;

INSERT INTO pluto_changes_applied
SELECT DISTINCT b.*
FROM pluto_input_research b, pluto a
WHERE b.bbl=a.bbl 
	AND b.field='unitsres' 
	AND b.old_value = a.unitsres;

-- Apply correction to PLUTO
UPDATE pluto a
SET unitsres = b.new_value,
	dcpedited = 't'
FROM pluto_input_research b
WHERE a.bbl = b.bbl
	AND b.field = 'unitsres'
	AND a.unitsres=b.old_value;