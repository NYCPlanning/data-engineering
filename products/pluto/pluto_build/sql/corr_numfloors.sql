-- Take the numfloors value from research table

-- Take note of corrections not applied to pluto
INSERT INTO pluto_changes_not_applied
SELECT DISTINCT b.*
FROM pluto_input_research b, pluto a
WHERE b.bbl=a.bbl 
	AND b.field='numfloors' 
	AND b.old_value <> a.numfloors;

INSERT INTO pluto_changes_applied
SELECT DISTINCT b.*
FROM pluto_input_research b, pluto a
WHERE b.bbl=a.bbl 
	AND b.field='numfloors' 
	AND b.old_value = a.numfloors;

-- Apply correction to PLUTO
UPDATE pluto a
SET numfloors = b.new_value,
	dcpedited = 't'
FROM pluto_input_research b
WHERE a.bbl = b.bbl
	AND b.field = 'numfloors'
	AND a.numfloors=b.old_value;
