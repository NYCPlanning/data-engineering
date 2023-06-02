DROP TABLE IF EXISTS facdb;
SELECT
    TRIM(UPPER(facdb_base.facname)) as facname,
    facdb_address.addressnum,
    facdb_address.streetname,
    facdb_address.address,
    facdb_boro.city,
    facdb_boro.zipcode,
    facdb_boro.boro,
    facdb_boro.borocode,
    facdb_spatial.bin::integer as bin,
    facdb_spatial.bbl::bigint as bbl,
    facdb_spatial.commboard as cd,
    facdb_spatial.nta2010,
    facdb_spatial.nta2020,
    facdb_spatial.council,
    facdb_spatial.schooldist,
    facdb_spatial.policeprct,
    facdb_spatial.ct2010,
    facdb_spatial.ct2020,
    TRIM(UPPER(regexp_replace(facdb_base.factype, '\s+', ' ', 'g'))) as factype,
    UPPER(facdb_classification.facsubgrp) as facsubgrp,
    UPPER(facdb_classification.facgroup) as facgroup,
    UPPER(facdb_classification.facdomain) as facdomain,
    facdb_classification.servarea,
    facdb_base.opname,
    facdb_agency.opabbrev,
    facdb_agency.optype,
    facdb_agency.overagency,
    facdb_agency.overabbrev,
    facdb_agency.overlevel,
    facdb_base.capacity,
    facdb_base.captype,
    facdb_geom.latitude,
    facdb_geom.longitude,
    facdb_geom.x as xcoord,
    facdb_geom.y as ycoord,
    facdb_base.source as datasource,
    facdb_base.uid,
    facdb_geom.geom
INTO facdb
FROM facdb_base
LEFT JOIN facdb_spatial 	ON facdb_base.uid = facdb_spatial.uid
LEFT JOIN facdb_boro 		ON facdb_base.uid = facdb_boro.uid
LEFT JOIN facdb_address 	ON facdb_base.uid = facdb_address.uid
LEFT JOIN facdb_classification 	ON facdb_base.uid = facdb_classification.uid
LEFT JOIN facdb_agency 		ON facdb_base.uid = facdb_agency.uid
LEFT JOIN facdb_geom 		ON facdb_base.uid = facdb_geom.uid;

-- Remove records where field = 'remove' in manual_corrections
INSERT INTO corrections_applied (uid, field)
    (SELECT
        uid,
        'remove' as field
    FROM manual_corrections
    WHERE uid IN (SELECT uid FROM facdb));

INSERT INTO corrections_not_applied (uid, field)
    (SELECT
        uid,
        'remove' as field
    FROM manual_corrections
    WHERE uid NOT IN (SELECT uid FROM facdb));

DELETE FROM facdb
WHERE uid IN (SELECT uid FROM corrections_applied WHERE field = 'remove');

CALL apply_correction('facdb', 'manual_corrections');
