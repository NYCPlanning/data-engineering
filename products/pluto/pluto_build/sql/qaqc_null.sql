DELETE FROM qaqc_null
WHERE
    pair = :'VERSION' || ' - ' || :'VERSION_PREV'
    AND condo::boolean = :CONDO
    AND mapped::boolean = :MAPPED;

INSERT INTO qaqc_null (
    SELECT
        :'VERSION' || ' - ' || :'VERSION_PREV' AS pair,
        :CONDO AS condo,
        :MAPPED AS mapped,
        count(*) AS total,
        sum(CASE WHEN a.borough IS NULL AND b.borough IS NOT NULL THEN 1 ELSE 0 END) AS borough,
        sum(CASE WHEN a.block IS NULL AND b.block IS NOT NULL THEN 1 ELSE 0 END) AS block,
        sum(CASE WHEN a.lot IS NULL AND b.lot IS NOT NULL THEN 1 ELSE 0 END) AS lot,
        sum(CASE WHEN a.cd IS NULL AND b.cd IS NOT NULL THEN 1 ELSE 0 END) AS cd,
        sum(CASE WHEN a.ct2010 IS NULL AND b.ct2010 IS NOT NULL THEN 1 ELSE 0 END) AS ct2010,
        sum(CASE WHEN a.cb2010 IS NULL AND b.cb2010 IS NOT NULL THEN 1 ELSE 0 END) AS cb2010,
        sum(CASE WHEN a.schooldist IS NULL AND b.schooldist IS NOT NULL THEN 1 ELSE 0 END) AS schooldist,
        sum(CASE WHEN a.council IS NULL AND b.council IS NOT NULL THEN 1 ELSE 0 END) AS council,
        sum(CASE WHEN a.zipcode IS NULL AND b.zipcode IS NOT NULL THEN 1 ELSE 0 END) AS zipcode,
        sum(CASE WHEN a.firecomp IS NULL AND b.firecomp IS NOT NULL THEN 1 ELSE 0 END) AS firecomp,
        sum(CASE WHEN a.policeprct IS NULL AND b.policeprct IS NOT NULL THEN 1 ELSE 0 END) AS policeprct,
        sum(CASE WHEN a.healtharea IS NULL AND b.healtharea IS NOT NULL THEN 1 ELSE 0 END) AS healtharea,
        sum(CASE WHEN a.sanitboro IS NULL AND b.sanitboro IS NOT NULL THEN 1 ELSE 0 END) AS sanitboro,
        sum(CASE WHEN a.sanitsub IS NULL AND b.sanitsub IS NOT NULL THEN 1 ELSE 0 END) AS sanitsub,
        sum(CASE WHEN a.address IS NULL AND b.address IS NOT NULL THEN 1 ELSE 0 END) AS address,
        sum(CASE WHEN a.zonedist1 IS NULL AND b.zonedist1 IS NOT NULL THEN 1 ELSE 0 END) AS zonedist1,
        sum(CASE WHEN a.zonedist2 IS NULL AND b.zonedist2 IS NOT NULL THEN 1 ELSE 0 END) AS zonedist2,
        sum(CASE WHEN a.zonedist3 IS NULL AND b.zonedist3 IS NOT NULL THEN 1 ELSE 0 END) AS zonedist3,
        sum(CASE WHEN a.zonedist4 IS NULL AND b.zonedist4 IS NOT NULL THEN 1 ELSE 0 END) AS zonedist4,
        sum(CASE WHEN a.overlay1 IS NULL AND b.overlay1 IS NOT NULL THEN 1 ELSE 0 END) AS overlay1,
        sum(CASE WHEN a.overlay2 IS NULL AND b.overlay2 IS NOT NULL THEN 1 ELSE 0 END) AS overlay2,
        sum(CASE WHEN a.spdist1 IS NULL AND b.spdist1 IS NOT NULL THEN 1 ELSE 0 END) AS spdist1,
        sum(CASE WHEN a.spdist2 IS NULL AND b.spdist2 IS NOT NULL THEN 1 ELSE 0 END) AS spdist2,
        sum(CASE WHEN a.spdist3 IS NULL AND b.spdist3 IS NOT NULL THEN 1 ELSE 0 END) AS spdist3,
        sum(CASE WHEN a.ltdheight IS NULL AND b.ltdheight IS NOT NULL THEN 1 ELSE 0 END) AS ltdheight,
        sum(CASE WHEN a.splitzone IS NULL AND b.splitzone IS NOT NULL THEN 1 ELSE 0 END) AS splitzone,
        sum(CASE WHEN a.bldgclass IS NULL AND b.bldgclass IS NOT NULL THEN 1 ELSE 0 END) AS bldgclass,
        sum(CASE WHEN a.landuse IS NULL AND b.landuse IS NOT NULL THEN 1 ELSE 0 END) AS landuse,
        sum(CASE WHEN a.easements IS NULL AND b.easements IS NOT NULL THEN 1 ELSE 0 END) AS easements,
        sum(CASE WHEN a.ownertype IS NULL AND b.ownertype IS NOT NULL THEN 1 ELSE 0 END) AS ownertype,
        sum(CASE WHEN a.ownername IS NULL AND b.ownername IS NOT NULL THEN 1 ELSE 0 END) AS ownername,
        sum(CASE WHEN a.lotarea IS NULL AND b.lotarea IS NOT NULL THEN 1 ELSE 0 END) AS lotarea,
        sum(CASE WHEN a.bldgarea IS NULL AND b.bldgarea IS NOT NULL THEN 1 ELSE 0 END) AS bldgarea,
        sum(CASE WHEN a.comarea IS NULL AND b.comarea IS NOT NULL THEN 1 ELSE 0 END) AS comarea,
        sum(CASE WHEN a.resarea IS NULL AND b.resarea IS NOT NULL THEN 1 ELSE 0 END) AS resarea,
        sum(CASE WHEN a.officearea IS NULL AND b.officearea IS NOT NULL THEN 1 ELSE 0 END) AS officearea,
        sum(CASE WHEN a.retailarea IS NULL AND b.retailarea IS NOT NULL THEN 1 ELSE 0 END) AS retailarea,
        sum(CASE WHEN a.garagearea IS NULL AND b.garagearea IS NOT NULL THEN 1 ELSE 0 END) AS garagearea,
        sum(CASE WHEN a.strgearea IS NULL AND b.strgearea IS NOT NULL THEN 1 ELSE 0 END) AS strgearea,
        sum(CASE WHEN a.factryarea IS NULL AND b.factryarea IS NOT NULL THEN 1 ELSE 0 END) AS factryarea,
        sum(CASE WHEN a.otherarea IS NULL AND b.otherarea IS NOT NULL THEN 1 ELSE 0 END) AS otherarea,
        sum(CASE WHEN a.areasource IS NULL AND b.areasource IS NOT NULL THEN 1 ELSE 0 END) AS areasource,
        sum(CASE WHEN a.numbldgs IS NULL AND b.numbldgs IS NOT NULL THEN 1 ELSE 0 END) AS numbldgs,
        sum(CASE WHEN a.numfloors IS NULL AND b.numfloors IS NOT NULL THEN 1 ELSE 0 END) AS numfloors,
        sum(CASE WHEN a.unitsres IS NULL AND b.unitsres IS NOT NULL THEN 1 ELSE 0 END) AS unitsres,
        sum(CASE WHEN a.unitstotal IS NULL AND b.unitstotal IS NOT NULL THEN 1 ELSE 0 END) AS unitstotal,
        sum(CASE WHEN a.lotfront IS NULL AND b.lotfront IS NOT NULL THEN 1 ELSE 0 END) AS lotfront,
        sum(CASE WHEN a.lotdepth IS NULL AND b.lotdepth IS NOT NULL THEN 1 ELSE 0 END) AS lotdepth,
        sum(CASE WHEN a.bldgfront IS NULL AND b.bldgfront IS NOT NULL THEN 1 ELSE 0 END) AS bldgfront,
        sum(CASE WHEN a.bldgdepth IS NULL AND b.bldgdepth IS NOT NULL THEN 1 ELSE 0 END) AS bldgdepth,
        sum(CASE WHEN a.ext IS NULL AND b.ext IS NOT NULL THEN 1 ELSE 0 END) AS ext,
        sum(CASE WHEN a.proxcode IS NULL AND b.proxcode IS NOT NULL THEN 1 ELSE 0 END) AS proxcode,
        sum(CASE WHEN a.irrlotcode IS NULL AND b.irrlotcode IS NOT NULL THEN 1 ELSE 0 END) AS irrlotcode,
        sum(CASE WHEN a.lottype IS NULL AND b.lottype IS NOT NULL THEN 1 ELSE 0 END) AS lottype,
        sum(CASE WHEN a.bsmtcode IS NULL AND b.bsmtcode IS NOT NULL THEN 1 ELSE 0 END) AS bsmtcode,
        sum(CASE WHEN a.assessland IS NULL AND b.assessland IS NOT NULL THEN 1 ELSE 0 END) AS assessland,
        sum(CASE WHEN a.assesstot IS NULL AND b.assesstot IS NOT NULL THEN 1 ELSE 0 END) AS assesstot,
        sum(CASE WHEN a.exempttot IS NULL AND b.exempttot IS NOT NULL THEN 1 ELSE 0 END) AS exempttot,
        sum(CASE WHEN a.yearbuilt IS NULL AND b.yearbuilt IS NOT NULL THEN 1 ELSE 0 END) AS yearbuilt,
        sum(CASE WHEN a.yearalter1 IS NULL AND b.yearalter1 IS NOT NULL THEN 1 ELSE 0 END) AS yearalter1,
        sum(CASE WHEN a.yearalter2 IS NULL AND b.yearalter2 IS NOT NULL THEN 1 ELSE 0 END) AS yearalter2,
        sum(CASE WHEN a.histdist IS NULL AND b.histdist IS NOT NULL THEN 1 ELSE 0 END) AS histdist,
        sum(CASE WHEN a.landmark IS NULL AND b.landmark IS NOT NULL THEN 1 ELSE 0 END) AS landmark,
        sum(CASE WHEN a.builtfar IS NULL AND b.builtfar IS NOT NULL THEN 1 ELSE 0 END) AS builtfar,
        sum(CASE WHEN a.residfar IS NULL AND b.residfar IS NOT NULL THEN 1 ELSE 0 END) AS residfar,
        sum(CASE WHEN a.commfar IS NULL AND b.commfar IS NOT NULL THEN 1 ELSE 0 END) AS commfar,
        sum(CASE WHEN a.facilfar IS NULL AND b.facilfar IS NOT NULL THEN 1 ELSE 0 END) AS facilfar,
        sum(CASE WHEN a.borocode IS NULL AND b.borocode IS NOT NULL THEN 1 ELSE 0 END) AS borocode,
        sum(CASE WHEN a.bbl IS NULL AND b.bbl IS NOT NULL THEN 1 ELSE 0 END) AS bbl,
        sum(CASE WHEN a.condono IS NULL AND b.condono IS NOT NULL THEN 1 ELSE 0 END) AS condono,
        sum(CASE WHEN a.tract2010 IS NULL AND b.tract2010 IS NOT NULL THEN 1 ELSE 0 END) AS tract2010,
        sum(CASE WHEN a.xcoord IS NULL AND b.xcoord IS NOT NULL THEN 1 ELSE 0 END) AS xcoord,
        sum(CASE WHEN a.ycoord IS NULL AND b.ycoord IS NOT NULL THEN 1 ELSE 0 END) AS ycoord,
        sum(CASE WHEN a.longitude IS NULL AND b.longitude IS NOT NULL THEN 1 ELSE 0 END) AS longitude,
        sum(CASE WHEN a.latitude IS NULL AND b.latitude IS NOT NULL THEN 1 ELSE 0 END) AS latitude,
        sum(CASE WHEN a.zonemap IS NULL AND b.zonemap IS NOT NULL THEN 1 ELSE 0 END) AS zonemap,
        sum(CASE WHEN a.zmcode IS NULL AND b.zmcode IS NOT NULL THEN 1 ELSE 0 END) AS zmcode,
        sum(CASE WHEN a.sanborn IS NULL AND b.sanborn IS NOT NULL THEN 1 ELSE 0 END) AS sanborn,
        sum(CASE WHEN a.taxmap IS NULL AND b.taxmap IS NOT NULL THEN 1 ELSE 0 END) AS taxmap,
        sum(CASE WHEN a.edesignum IS NULL AND b.edesignum IS NOT NULL THEN 1 ELSE 0 END) AS edesignum,
        sum(CASE WHEN a.appbbl IS NULL AND b.appbbl IS NOT NULL THEN 1 ELSE 0 END) AS appbbl,
        sum(CASE WHEN a.appdate IS NULL AND b.appdate IS NOT NULL THEN 1 ELSE 0 END) AS appdate,
        sum(CASE WHEN a.plutomapid IS NULL AND b.plutomapid IS NOT NULL THEN 1 ELSE 0 END) AS plutomapid,
        sum(CASE WHEN a.version IS NULL AND b.version IS NOT NULL THEN 1 ELSE 0 END) AS version,
        sum(CASE WHEN a.sanitdistrict IS NULL AND b.sanitdistrict IS NOT NULL THEN 1 ELSE 0 END) AS sanitdistrict,
        sum(
            CASE WHEN a.healthcenterdistrict IS NULL AND b.healthcenterdistrict IS NOT NULL THEN 1 ELSE 0 END
        ) AS healthcenterdistrict,
        sum(CASE WHEN a.firm07_flag IS NULL AND b.firm07_flag IS NOT NULL THEN 1 ELSE 0 END) AS firm07_flag,
        sum(CASE WHEN a.pfirm15_flag IS NULL AND b.pfirm15_flag IS NOT NULL THEN 1 ELSE 0 END) AS pfirm15_flag
    FROM archive_pluto AS a
    INNER JOIN previous_pluto AS b
        ON (a.bbl::float::bigint = b.bbl::float::bigint)
    :CONDITION
);
