/*

*** PLANNED DEPRECATION ***

in 20v5, the inwood rezoning field will 
be kept and left as NULL for all records. 
For future releases, this field would be 
completely removed.

*/

-- add a notes column
ALTER TABLE pluto 
DROP COLUMN IF EXISTS notes;
ALTER TABLE pluto 
ADD COLUMN notes text;

-- populate notes with 1 where lot interesects inwood rezoning area
-- except for 4 lots
-- UPDATE pluto a
-- SET notes = '1'
-- FROM dcp_zoningmapamendments b
-- WHERE b.project_na = 'Inwood Rezoning'
-- AND ST_Intersects(a.geom,b.geom)
-- AND bbl != '1022552000' 
-- AND bbl != '1022550001'
-- AND bbl != '1021890001'
-- AND bbl != '1021970001';