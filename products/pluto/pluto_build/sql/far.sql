-- calculate the built FAR
-- divide the total building are (bldgarea) by the total lot area (lotarea)
UPDATE pluto
SET builtfar = round(bldgarea::numeric / lotarea::numeric, 2)
WHERE lotarea != '0' AND lotarea IS NOT NULL;

-- using dcp_zoning_maxfar maintained by zoning division
-- base on zoning district 1
UPDATE pluto a
SET
    residfar = coalesce(b.residfar::double precision, 0::double precision),
    commfar = coalesce(b.commfar::double precision, 0::double precision),
    facilfar = coalesce(b.facilfar::double precision, 0::double precision),
    affresfar = coalesce(b.affresfar::double precision, 0::double precision)
    mnffar = coalesce(b.mnffar::double precision, 0::double precision),
FROM pluto AS p
LEFT JOIN dcp_zoning_maxfar AS b ON p.zonedist1 = b.zonedist
WHERE a.bbl = p.bbl;
