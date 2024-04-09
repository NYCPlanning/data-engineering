-- set the sanitation boro
UPDATE pluto
SET sanitboro = left(sanitdistrict, 1)
WHERE sanitdistrict IS NOT NULL;

-- set the sanitdistrict to not include sanitboro
UPDATE pluto
SET sanitdistrict = right(sanitdistrict, 2)
WHERE sanitdistrict IS NOT NULL;
