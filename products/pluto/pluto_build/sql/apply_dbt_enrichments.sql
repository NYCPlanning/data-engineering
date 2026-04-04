-- Apply DBT enrichments from pluto_enriched table to pluto
-- This batch UPDATE applies all the business logic computed by dbt intermediate models

UPDATE pluto
SET
    builtfar = pe.builtfar,
    residfar = pe.residfar,
    commfar = pe.commfar,
    facilfar = pe.facilfar,
    affresfar = pe.affresfar,
    mnffar = pe.mnffar,
    irrlotcode = pe.irrlotcode,
    condono = pe.condono,
    histdist = pe.histdist,
    landmark = pe.landmark,
    lotdepth = pe.lotdepth,
    numfloors = pe.numfloors,
    lotarea = pe.lotarea,
    sanborn = pe.sanborn,
    latitude = pe.latitude,
    longitude = pe.longitude,
    centroid = pe.centroid,
    mih_opt1 = pe.mih_opt1,
    mih_opt2 = pe.mih_opt2,
    mih_opt3 = pe.mih_opt3,
    mih_opt4 = pe.mih_opt4,
    trnstzone = pe.trnstzone
FROM pluto_enriched AS pe
WHERE pluto.bbl = pe.bbl;
