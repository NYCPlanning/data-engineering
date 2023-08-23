-- Take the year built from researched lpc_historic_districts table

INSERT INTO pluto_changes_not_applied
SELECT DISTINCT b.*
FROM pluto_input_research b, pluto a
WHERE b.bbl=a.bbl 
	AND b.field='yearbuilt'
	AND b.old_value <> a.yearbuilt;

INSERT INTO pluto_changes_applied
SELECT DISTINCT b.*
FROM pluto_input_research b, pluto a
WHERE b.bbl=a.bbl 
	AND b.field='yearbuilt' 
	AND b.old_value = a.yearbuilt;

-- Apply correction to PLUTO
UPDATE pluto a
SET yearbuilt = b.new_value,
	dcpedited = 't'
FROM pluto_input_research b
WHERE a.bbl = b.bbl
	AND b.field = 'yearbuilt'
	AND a.yearbuilt=b.old_value;