-- add a notes column
ALTER TABLE dcp_zoning_taxlot 
DROP COLUMN IF EXISTS notes;
ALTER TABLE dcp_zoning_taxlot 
ADD COLUMN notes text;
-- populate notes with 1 where lot interesects inwood rezoning area
-- except for 4 lots

UPDATE dcp_zoning_taxlot a
SET notes = '1'
WHERE a.bbl in (
	SELECT b.bbl
	FROM dof_dtm b, dcp_zoningmapamendments c
	WHERE b.geom is not null
		AND c.project_na = 'Inwood Rezoning'
		AND ST_Intersects(b.geom, c.geom))
AND a.bbl != '1022552000'
AND a.bbl != '1022550001'
AND a.bbl != '1021890001'
AND a.bbl != '1021970001';