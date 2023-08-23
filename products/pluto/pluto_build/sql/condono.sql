-- update the format of the condo number
-- remove the borough code from the beginning of the condo number
-- remove leading zeros
UPDATE pluto
SET condono = (RIGHT(condono,5)::numeric)::text;