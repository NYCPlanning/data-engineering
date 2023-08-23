-- Adding on data from geocodes input table
-- might need to get fire company type
UPDATE pluto a
SET cd = b.cd,
	bct2020 = (CASE WHEN b.ct2020::numeric = 0 THEN NULL ELSE a.borocode||b.ct2020 END),
	bctcb2020 = (CASE WHEN b.ct2020::numeric = 0 THEN NULL ELSE a.borocode||b.ct2020||b.cb2020 END),
	ct2010 = b.ct2010,
	tract2010 = b.ct2010,
	cb2010 = b.cb2010,
	schooldist = b.schooldist,
	-- council = ltrim(b.council, '0'),
	zipcode = b.zipcode,
	firecomp = b.firecomp,
	policeprct = ltrim(b.policeprct, '0'),
	healthcenterdistrict = b.healthcenterdistrict,
	healtharea = ltrim(b.healtharea, '0'),
	sanitdistrict = b.sanitdistrict,
	sanitsub = b.sanitsub,
	taxmap = b.taxmap,
	sanborn = b.sanbornmapidentifier,
	address = concat(b.housenum_lo,' ',b.street_name),
	ycoord = ltrim(b.ycoord, '0'),
	xcoord = ltrim(b.xcoord, '0')
FROM pluto_rpad_geo b
WHERE a.bbl = b.primebbl AND a.lot NOT LIKE '75%';

--updating the building code if it was not updated in alloceted
UPDATE pluto a
SET bldgclass = b.bldgcl
FROM pluto_rpad_geo b
WHERE a.bbl = b.primebbl 
AND a.lot NOT LIKE '75%'
AND bldgclass IS NULL;

-- If lot is a condo lot, instead of joining on
-- primebbl, we join on borough||block||lot

UPDATE pluto a
SET cd = b.cd,
	bct2020 = (CASE WHEN b.ct2020::numeric = 0 THEN NULL ELSE a.borocode||b.ct2020 END),
	bctcb2020 = (CASE WHEN b.ct2020::numeric = 0 THEN NULL ELSE a.borocode||b.ct2020||b.cb2020 END),
	ct2010 = b.ct2010,
	tract2010 = b.ct2010,
	cb2010 = b.cb2010,
	schooldist = b.schooldist,
	-- council = ltrim(b.council, '0'),
	zipcode = b.zipcode,
	firecomp = b.firecomp,
	policeprct = ltrim(b.policeprct, '0'),
	healthcenterdistrict = b.healthcenterdistrict,
	healtharea = ltrim(b.healtharea, '0'),
	sanitdistrict = b.sanitdistrict,
	sanitsub = b.sanitsub,
	taxmap = b.taxmap,
	sanborn = b.sanbornmapidentifier,
	address = concat(b.housenum_lo,' ',b.street_name),
	ycoord = ltrim(b.ycoord, '0'),
	xcoord = ltrim(b.xcoord, '0')
FROM pluto_rpad_geo b
WHERE a.bbl = b.borough||b.block||b.lot AND a.lot LIKE '75%';

--updating the building code if it was not updated in alloceted
UPDATE pluto a
SET bldgclass = b.bldgcl
FROM pluto_rpad_geo b
WHERE a.bbl = b.borough||b.block||b.lot
AND a.lot LIKE '75%'
AND bldgclass IS NULL;