-- calculate the built FAR
-- divide the total building are (bldgarea) by the total lot area (lotarea)
UPDATE pluto
SET builtfar = round(bldgarea::numeric / lotarea::numeric, 2)
WHERE lotarea != '0' AND lotarea IS NOT NULL;

-- using dcp_zoning_maxfar maintained by zoning division
-- base on zoning district 1
UPDATE pluto a
SET
    residfar = b.residfar,
    commfar = b.commfar,
    facilfar = b.facilfar
FROM dcp_zoning_maxfar AS b
WHERE a.zonedist1 = b.zonedist;
-- zoning district 1 with / first p art 
UPDATE pluto a
SET
    residfar = (coalesce(a.residfar, b.residfar)),
    commfar = (coalesce(a.commfar, b.commfar)),
    facilfar = (coalesce(a.facilfar, b.facilfar))
FROM dcp_zoning_maxfar AS b
WHERE split_part(a.zonedist1, '/', 1) = b.zonedist;

-- zoning district 1 with / second part 
UPDATE pluto a
SET
    residfar = (coalesce(a.residfar, b.residfar)),
    commfar = (coalesce(a.commfar, b.commfar)),
    facilfar = (coalesce(a.facilfar, b.facilfar))
FROM dcp_zoning_maxfar AS b
WHERE split_part(a.zonedist1, '/', 2) = b.zonedist;

-- base on zoning district 2
UPDATE pluto a
SET
    residfar = (coalesce(a.residfar, b.residfar)),
    commfar = (coalesce(a.commfar, b.commfar)),
    facilfar = (coalesce(a.facilfar, b.facilfar))
FROM dcp_zoning_maxfar AS b
WHERE a.zonedist2 = b.zonedist;

-- zoning district 2 with / first part 
UPDATE pluto a
SET
    residfar = (coalesce(a.residfar, b.residfar)),
    commfar = (coalesce(a.commfar, b.commfar)),
    facilfar = (coalesce(a.facilfar, b.facilfar))
FROM dcp_zoning_maxfar AS b
WHERE split_part(a.zonedist2, '/', 1) = b.zonedist;

-- zoning district 2 with / second part 
UPDATE pluto a
SET
    residfar = (coalesce(a.residfar, b.residfar)),
    commfar = (coalesce(a.commfar, b.commfar)),
    facilfar = (coalesce(a.facilfar, b.facilfar))
FROM dcp_zoning_maxfar AS b
WHERE split_part(a.zonedist2, '/', 2) = b.zonedist;

-- base on zoning district 3
UPDATE pluto a
SET
    residfar = (coalesce(a.residfar, b.residfar)),
    commfar = (coalesce(a.commfar, b.commfar)),
    facilfar = (coalesce(a.facilfar, b.facilfar))
FROM dcp_zoning_maxfar AS b
WHERE a.zonedist3 = b.zonedist;

-- zoning district 3 with / first part 
UPDATE pluto a
SET
    residfar = (coalesce(a.residfar, b.residfar)),
    commfar = (coalesce(a.commfar, b.commfar)),
    facilfar = (coalesce(a.facilfar, b.facilfar))
FROM dcp_zoning_maxfar AS b
WHERE split_part(a.zonedist3, '/', 1) = b.zonedist;

-- zoning district 3 with / second part 
UPDATE pluto a
SET
    residfar = (coalesce(a.residfar, b.residfar)),
    commfar = (coalesce(a.commfar, b.commfar)),
    facilfar = (coalesce(a.facilfar, b.facilfar))
FROM dcp_zoning_maxfar AS b
WHERE split_part(a.zonedist3, '/', 2) = b.zonedist;

-- make NULLs zeros and make values numeric
UPDATE pluto a
SET
    residfar
    = (CASE WHEN a.residfar IS NULL OR a.residfar = '-' THEN 0::double precision ELSE a.residfar::double precision END),
    commfar
    = (CASE WHEN a.commfar IS NULL OR a.commfar = '-' THEN 0::double precision ELSE a.commfar::double precision END),
    facilfar = (CASE WHEN a.facilfar IS NULL OR a.facilfar = '-' THEN 0 ELSE a.facilfar::double precision END);
