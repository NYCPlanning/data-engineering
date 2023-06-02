-- Take the community district value from research table
INSERT INTO pluto_changes_not_applied
SELECT DISTINCT b.*
FROM pluto_input_research b, pluto a
WHERE b.bbl=a.bbl 
	AND b.field='cd'
	AND b.old_value <> a.cd;

INSERT INTO pluto_changes_applied
SELECT DISTINCT b.*
FROM pluto_input_research b, pluto a
WHERE b.bbl=a.bbl 
	AND b.field='cd' 
	AND b.old_value = a.cd;

-- Apply correction to PLUTO
UPDATE pluto a
SET cd = b.new_value,
	dcpedited = 't'
FROM pluto_input_research b
WHERE a.bbl = b.bbl
	AND b.field = 'cd'
	AND a.cd=b.old_value;