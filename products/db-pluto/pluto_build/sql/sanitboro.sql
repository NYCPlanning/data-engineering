-- set the sanitation boro
UPDATE pluto
SET sanitboro = LEFT(sanitdistrict,1)
WHERE sanitdistrict IS NOT NULL;

-- set the sanitdistrict to not include sanitboro
UPDATE pluto
SET sanitdistrict = RIGHT(sanitdistrict,2)
WHERE sanitdistrict IS NOT NULL;