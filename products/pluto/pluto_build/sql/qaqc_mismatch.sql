DELETE FROM qaqc_mismatch
WHERE
    pair = :'VERSION' || ' - ' || :'VERSION_PREV'
    AND condo::boolean = :CONDO
    AND mapped::boolean = :MAPPED;

INSERT INTO qaqc_mismatch (
    SELECT
        :'VERSION' || ' - ' || :'VERSION_PREV' AS pair,
        :CONDO AS condo,
        :MAPPED AS mapped,
        count(*) AS total,
        count(*) FILTER (WHERE a.borough IS DISTINCT FROM b.borough) AS borough,
        count(*) FILTER (WHERE a.block::varchar IS DISTINCT FROM b.block::varchar) AS block,
        count(*) FILTER (WHERE a.lot::varchar IS DISTINCT FROM b.lot::varchar) AS lot,
        count(*) FILTER (WHERE a.cd::varchar IS DISTINCT FROM b.cd::varchar) AS cd,
        count(*) FILTER (WHERE a.ct2010::varchar IS DISTINCT FROM b.ct2010::varchar) AS ct2010,
        count(*) FILTER (WHERE a.cb2010 IS DISTINCT FROM b.cb2010) AS cb2010,
        count(*) FILTER (WHERE a.schooldist IS DISTINCT FROM b.schooldist) AS schooldist,
        count(*) FILTER (WHERE a.council::varchar IS DISTINCT FROM b.council::varchar) AS council,
        count(*) FILTER (WHERE a.zipcode::varchar IS DISTINCT FROM b.zipcode::varchar) AS zipcode,
        count(*) FILTER (WHERE a.firecomp::varchar IS DISTINCT FROM b.firecomp::varchar) AS firecomp,
        count(*) FILTER (WHERE a.policeprct::varchar IS DISTINCT FROM b.policeprct::varchar) AS policeprct,
        count(*) FILTER (WHERE a.healtharea::varchar IS DISTINCT FROM b.healtharea::varchar) AS healtharea,
        count(*) FILTER (WHERE a.sanitboro::varchar IS DISTINCT FROM b.sanitboro::varchar) AS sanitboro,
        count(*) FILTER (WHERE a.sanitsub::varchar IS DISTINCT FROM b.sanitsub::varchar) AS sanitsub,
        count(*) FILTER (WHERE trim(a.address) IS DISTINCT FROM trim(b.address)) AS address,
        count(*) FILTER (WHERE a.zonedist1 IS DISTINCT FROM b.zonedist1) AS zonedist1,
        count(*) FILTER (WHERE a.zonedist2 IS DISTINCT FROM b.zonedist2) AS zonedist2,
        count(*) FILTER (WHERE a.zonedist3 IS DISTINCT FROM b.zonedist3) AS zonedist3,
        count(*) FILTER (WHERE a.zonedist4 IS DISTINCT FROM b.zonedist4) AS zonedist4,
        count(*) FILTER (WHERE a.overlay1 IS DISTINCT FROM b.overlay1) AS overlay1,
        count(*) FILTER (WHERE a.overlay2 IS DISTINCT FROM b.overlay2) AS overlay2,
        count(*) FILTER (WHERE a.spdist1 IS DISTINCT FROM b.spdist1) AS spdist1,
        count(*) FILTER (WHERE a.spdist2 IS DISTINCT FROM b.spdist2) AS spdist2,
        count(*) FILTER (WHERE a.spdist3 IS DISTINCT FROM b.spdist3) AS spdist3,
        count(*) FILTER (WHERE a.ltdheight IS DISTINCT FROM b.ltdheight) AS ltdheight,
        count(*) FILTER (WHERE a.splitzone IS DISTINCT FROM b.splitzone) AS splitzone,
        count(*) FILTER (WHERE a.bldgclass IS DISTINCT FROM b.bldgclass) AS bldgclass,
        count(*) FILTER (WHERE a.landuse IS DISTINCT FROM b.landuse) AS landuse,
        count(*) FILTER (WHERE a.easements::numeric IS DISTINCT FROM b.easements::numeric) AS easements,
        count(*) FILTER (WHERE a.ownertype IS DISTINCT FROM b.ownertype) AS ownertype,
        count(*) FILTER (WHERE a.ownername IS DISTINCT FROM b.ownername) AS ownername,
        count(*) FILTER (
            WHERE abs(a.lotarea::numeric - b.lotarea::numeric) >= 5
            OR ((a.lotarea IS NULL)::int + (b.lotarea IS NULL)::int) = 1
        ) AS lotarea,
        count(*) FILTER (
            WHERE abs(a.bldgarea::numeric - b.bldgarea::numeric) >= 5
            OR ((a.bldgarea IS NULL)::int + (b.bldgarea IS NULL)::int) = 1
        ) AS bldgarea,
        count(*) FILTER (
            WHERE abs(a.comarea::numeric - b.comarea::numeric) >= 5
            OR ((a.comarea IS NULL)::int + (b.comarea IS NULL)::int) = 1
        ) AS comarea,
        count(*) FILTER (
            WHERE abs(a.resarea::numeric - b.resarea::numeric) >= 5
            OR ((a.resarea IS NULL)::int + (b.resarea IS NULL)::int) = 1
        ) AS resarea,
        count(*) FILTER (
            WHERE abs(a.officearea::numeric - b.officearea::numeric) >= 5
            OR ((a.officearea IS NULL)::int + (b.officearea IS NULL)::int) = 1
        ) AS officearea,
        count(*) FILTER (
            WHERE abs(a.retailarea::numeric - b.retailarea::numeric) >= 5
            OR ((a.retailarea IS NULL)::int + (b.retailarea IS NULL)::int) = 1
        ) AS retailarea,
        count(*) FILTER (
            WHERE abs(a.garagearea::numeric - b.garagearea::numeric) >= 5
            OR ((a.garagearea IS NULL)::int + (b.garagearea IS NULL)::int) = 1
        ) AS garagearea,
        count(*) FILTER (
            WHERE abs(a.strgearea::numeric - b.strgearea::numeric) >= 5
            OR ((a.strgearea IS NULL)::int + (b.strgearea IS NULL)::int) = 1
        ) AS strgearea,
        count(*) FILTER (
            WHERE abs(a.factryarea::numeric - b.factryarea::numeric) >= 5
            OR ((a.factryarea IS NULL)::int + (b.factryarea IS NULL)::int) = 1
        ) AS factryarea,
        count(*) FILTER (
            WHERE abs(a.otherarea::numeric - b.otherarea::numeric) >= 5
            OR ((a.otherarea IS NULL)::int + (b.otherarea IS NULL)::int) = 1
        ) AS otherarea,
        count(*) FILTER (WHERE a.areasource IS DISTINCT FROM b.areasource) AS areasource,
        count(*) FILTER (WHERE a.numbldgs::numeric IS DISTINCT FROM b.numbldgs::numeric) AS numbldgs,
        count(*) FILTER (WHERE a.numfloors::numeric IS DISTINCT FROM b.numfloors::numeric) AS numfloors,
        count(*) FILTER (WHERE a.unitsres::numeric IS DISTINCT FROM b.unitsres::numeric) AS unitsres,
        count(*) FILTER (WHERE a.unitstotal::numeric IS DISTINCT FROM b.unitstotal::numeric) AS unitstotal,
        count(*) FILTER (
            WHERE abs(a.lotfront::numeric - b.lotfront::numeric) >= 5
            OR ((a.lotfront IS NULL)::int + (b.lotfront IS NULL)::int) = 1
        ) AS lotfront,
        count(*) FILTER (
            WHERE abs(a.lotdepth::numeric - b.lotdepth::numeric) >= 5
            OR ((a.lotdepth IS NULL)::int + (b.lotdepth IS NULL)::int) = 1
        ) AS lotdepth,
        count(*) FILTER (
            WHERE abs(a.bldgfront::numeric - b.bldgfront::numeric) >= 5
            OR ((a.bldgfront IS NULL)::int + (b.bldgfront IS NULL)::int) = 1
        ) AS bldgfront,
        count(*) FILTER (
            WHERE abs(a.bldgdepth::numeric - b.bldgdepth::numeric) >= 5
            OR ((a.bldgdepth IS NULL)::int + (b.bldgdepth IS NULL)::int) = 1
        ) AS bldgdepth,
        count(*) FILTER (WHERE a.ext IS DISTINCT FROM b.ext) AS ext,
        count(*) FILTER (WHERE a.proxcode IS DISTINCT FROM b.proxcode) AS proxcode,
        count(*) FILTER (WHERE a.irrlotcode IS DISTINCT FROM b.irrlotcode) AS irrlotcode,
        count(*) FILTER (WHERE a.lottype IS DISTINCT FROM b.lottype) AS lottype,
        count(*) FILTER (WHERE a.bsmtcode IS DISTINCT FROM b.bsmtcode) AS bsmtcode,
        count(*) FILTER (
            WHERE abs(a.assessland::numeric - b.assessland::numeric) >= 10
            OR ((a.assessland IS NULL)::int + (b.assessland IS NULL)::int) = 1
        ) AS assessland,
        count(*) FILTER (
            WHERE abs(a.assesstot::numeric - b.assesstot::numeric) >= 10
            OR ((a.assesstot IS NULL)::int + (b.assesstot IS NULL)::int) = 1
        ) AS assesstot,
        count(*) FILTER (
            WHERE abs(a.exempttot::numeric - b.exempttot::numeric) >= 10
            OR ((a.exempttot IS NULL)::int + (b.exempttot IS NULL)::int) = 1
        ) AS exempttot,
        count(*) FILTER (WHERE a.yearbuilt::numeric IS DISTINCT FROM b.yearbuilt::numeric) AS yearbuilt,
        count(*) FILTER (WHERE a.yearalter1::numeric IS DISTINCT FROM b.yearalter1::numeric) AS yearalter1,
        count(*) FILTER (WHERE a.yearalter2::numeric IS DISTINCT FROM b.yearalter2::numeric) AS yearalter2,
        count(*) FILTER (WHERE a.histdist IS DISTINCT FROM b.histdist) AS histdist,
        count(*) FILTER (WHERE a.landmark IS DISTINCT FROM b.landmark) AS landmark,
        count(*) FILTER (
            WHERE a.builtfar::double precision IS DISTINCT FROM
            b.builtfar::double precision
        ) AS builtfar,
        count(*) FILTER (
            WHERE a.residfar::double precision IS DISTINCT FROM
            b.residfar::double precision
        ) AS residfar,
        count(*) FILTER (
            WHERE a.commfar::double precision IS DISTINCT FROM
            b.commfar::double precision
        ) AS commfar,
        count(*) FILTER (
            WHERE a.facilfar::double precision IS DISTINCT FROM
            b.facilfar::double precision
        ) AS facilfar,
        count(*) FILTER (WHERE a.borocode::numeric IS DISTINCT FROM b.borocode::numeric) AS borocode,
        0 AS bbl, -- can't have bbl changes when we're joining on bbl. But included for backwards compatibility
        count(*) FILTER (WHERE a.condono::numeric IS DISTINCT FROM b.condono::numeric) AS condono,
        count(*) FILTER (WHERE a.tract2010 IS DISTINCT FROM b.tract2010) AS tract2010,
        count(*) FILTER (
            WHERE abs(a.xcoord::numeric - b.xcoord::numeric) >= 1
            OR ((a.xcoord IS NULL)::int + (b.xcoord IS NULL)::int) = 1
        ) AS xcoord,
        count(*) FILTER (
            WHERE abs(a.ycoord::numeric - b.ycoord::numeric) >= 1
            OR ((a.ycoord IS NULL)::int + (b.ycoord IS NULL)::int) = 1
        ) AS ycoord,
        count(*) FILTER (
            WHERE abs(a.latitude::numeric - b.latitude::numeric) >= .0001
            OR ((a.latitude IS NULL)::int + (b.latitude IS NULL)::int) = 1
        ) AS latitude,
        count(*) FILTER (
            WHERE abs(a.longitude::numeric - b.longitude::numeric) >= .0001
            OR ((a.longitude IS NULL)::int + (b.longitude IS NULL)::int) = 1
        ) AS longitude,
        count(*) FILTER (WHERE a.zonemap IS DISTINCT FROM b.zonemap) AS zonemap,
        count(*) FILTER (WHERE a.zmcode IS DISTINCT FROM b.zmcode) AS zmcode,
        count(*) FILTER (WHERE a.sanborn IS DISTINCT FROM b.sanborn) AS sanborn,
        count(*) FILTER (WHERE a.taxmap IS DISTINCT FROM b.taxmap) AS taxmap,
        count(*) FILTER (WHERE a.edesignum IS DISTINCT FROM b.edesignum) AS edesignum,
        count(*) FILTER (
            WHERE a.appbbl::double precision
            IS DISTINCT FROM b.appbbl::double precision
        ) AS appbbl,
        count(*) FILTER (WHERE a.appdate IS DISTINCT FROM b.appdate) AS appdate,
        count(*) FILTER (WHERE a.plutomapid IS DISTINCT FROM b.plutomapid) AS plutomapid,
        count(*) FILTER (WHERE a.sanitdistrict IS DISTINCT FROM b.sanitdistrict) AS sanitdistrict,
        count(*) FILTER (
            WHERE a.healthcenterdistrict::numeric
            IS DISTINCT FROM b.healthcenterdistrict::numeric
        ) AS healthcenterdistrict,
        count(*) FILTER (WHERE a.firm07_flag IS DISTINCT FROM b.firm07_flag) AS firm07_flag,
        count(*) FILTER (WHERE a.pfirm15_flag IS DISTINCT FROM b.pfirm15_flag) AS pfirm15_flag
    FROM archive_pluto AS a
    INNER JOIN previous_pluto AS b
        ON (a.bbl::float::bigint = b.bbl::float::bigint)
    :CONDITION
)
