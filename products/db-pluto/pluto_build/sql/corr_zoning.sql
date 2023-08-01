-- Take the zoning values from research table

INSERT INTO pluto_changes_not_applied
SELECT DISTINCT b.*
FROM pluto_input_research b, pluto a
WHERE b.bbl=a.bbl 
	AND b.field='zonedist2' 
	AND b.old_value <> a.zonedist2;

INSERT INTO pluto_changes_applied
SELECT DISTINCT b.*
FROM pluto_input_research b, pluto a
WHERE b.bbl=a.bbl 
	AND b.field='zonedist2' 
	AND b.old_value = a.zonedist2;

-- Apply correction to PLUTO
UPDATE pluto a
SET zonedist2 = b.new_value,
	dcpedited = 't'
FROM pluto_input_research b
WHERE a.bbl = b.bbl
	AND b.field = 'zonedist2'
	AND a.zonedist2=b.old_value;

-- Take the special district value from research table

INSERT INTO pluto_changes_not_applied
SELECT DISTINCT b.*
FROM pluto_input_research b, pluto a
WHERE b.bbl=a.bbl 
	AND b.field='spdist1' 
	AND b.old_value <> a.spdist1;

INSERT INTO pluto_changes_applied
SELECT DISTINCT b.*
FROM pluto_input_research b, pluto a
WHERE b.bbl=a.bbl 
	AND b.field='spdist1' 
	AND b.old_value = a.spdist1;

-- Apply correction to PLUTO
UPDATE pluto a
SET spdist1 = b.new_value,
	dcpedited = 't'
FROM pluto_input_research b
WHERE a.bbl = b.bbl
	AND b.field = 'spdist1'
	AND a.spdist1=b.old_value;