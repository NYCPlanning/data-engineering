-- Setting the lot area (land_area) field in pluto_rpad_geo
-- if lot area = 0 or is NULL and if lot is not irregular and 
-- and if lot depth and lot front have valid values then 
-- multiply lot front x lot depth to get lot area
UPDATE pluto_rpad_geo b
SET land_area = (a.lfft * a.ldft)
FROM pluto_rpad_geo a
WHERE (a.land_area IS NULL OR a.land_area = '0') 
	AND a.irreg <> 'I'
	AND a.lfft > 0 
	AND a.ldft > 0
	AND a.boro||a.tb||a.tl=b.boro||b.tb||b.tl;

-- if lot area = 0 or is NULL 
-- and lot depth =  ACRE 
-- then lot area = lot front x 43560 + .5
-- there are no more values name ACRE --
-- UPDATE pluto_rpad_geo b
-- SET land_area = (a.lfft * 43560 + 0.5)
-- FROM pluto_rpad_geo a
-- WHERE (a.land_area IS NULL OR a.land_area = '0')
	-- AND upper(a.ldft) LIKE '%ACRE%'
	-- AND a.lfft > 0
	-- AND a.boro||a.tb||a.tl=b.boro||b.tb||b.tl;