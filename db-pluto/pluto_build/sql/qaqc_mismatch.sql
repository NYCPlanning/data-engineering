DELETE FROM qaqc_mismatch 
WHERE pair = :'VERSION'||' - '||:'VERSION_PREV' 
AND CONDO::boolean = :CONDO
AND MAPPED::boolean = :MAPPED;

INSERT INTO qaqc_mismatch (
SELECT
    :'VERSION'||' - '||:'VERSION_PREV' as pair, 
	:CONDO as condo,
    :MAPPED as mapped,
    count(*) as total,
    count(*) FILTER (WHERE a.borough IS DISTINCT FROM b.borough) as borough,
    count(*) FILTER (WHERE a.block::varchar IS DISTINCT FROM b.block::varchar) as block,
    count(*) FILTER (WHERE a.lot::varchar IS DISTINCT FROM b.lot::varchar) as lot,
    count(*) FILTER (WHERE a.cd::varchar IS DISTINCT FROM b.cd::varchar) as cd,
    count(*) FILTER (WHERE a.ct2010::varchar IS DISTINCT FROM b.ct2010::varchar) as ct2010,
    count(*) FILTER (WHERE a.cb2010 IS DISTINCT FROM b.cb2010) as cb2010,
    count(*) FILTER (WHERE a.schooldist IS DISTINCT FROM b.schooldist) as schooldist,
    count(*) FILTER (WHERE a.council::varchar IS DISTINCT FROM b.council::varchar) as council,
    count(*) FILTER (WHERE a.zipcode::varchar IS DISTINCT FROM b.zipcode::varchar) as zipcode,
    count(*) FILTER (WHERE a.firecomp::varchar IS DISTINCT FROM b.firecomp::varchar) as firecomp,
    count(*) FILTER (WHERE a.policeprct::varchar IS DISTINCT FROM b.policeprct::varchar) as policeprct,
    count(*) FILTER (WHERE a.healtharea::varchar IS DISTINCT FROM b.healtharea::varchar) as healtharea,
    count(*) FILTER (WHERE a.sanitboro::varchar IS DISTINCT FROM b.sanitboro::varchar) as sanitboro,
    count(*) FILTER (WHERE a.sanitsub::varchar IS DISTINCT FROM b.sanitsub::varchar) as sanitsub,
    count(*) FILTER (WHERE trim(a.address) IS DISTINCT FROM trim(b.address)) as address,
    count(*) FILTER (WHERE a.zonedist1 IS DISTINCT FROM b.zonedist1) as zonedist1,
    count(*) FILTER (WHERE a.zonedist2 IS DISTINCT FROM b.zonedist2) as zonedist2,
    count(*) FILTER (WHERE a.zonedist3 IS DISTINCT FROM b.zonedist3) as zonedist3,
    count(*) FILTER (WHERE a.zonedist4 IS DISTINCT FROM b.zonedist4) as zonedist4,
    count(*) FILTER (WHERE a.overlay1 IS DISTINCT FROM b.overlay1) as overlay1,
    count(*) FILTER (WHERE a.overlay2 IS DISTINCT FROM b.overlay2) as overlay2,
    count(*) FILTER (WHERE a.spdist1 IS DISTINCT FROM b.spdist1) as spdist1,
    count(*) FILTER (WHERE a.spdist2 IS DISTINCT FROM b.spdist2) as spdist2,
    count(*) FILTER (WHERE a.spdist3 IS DISTINCT FROM b.spdist3) as spdist3,
    count(*) FILTER (WHERE a.ltdheight IS DISTINCT FROM b.ltdheight) as ltdheight,
    count(*) FILTER (WHERE a.splitzone IS DISTINCT FROM b.splitzone) as splitzone,
    count(*) FILTER (WHERE a.bldgclass IS DISTINCT FROM b.bldgclass) as bldgclass,
    count(*) FILTER (WHERE a.landuse IS DISTINCT FROM b.landuse) as landuse,
    count(*) FILTER (WHERE a.easements::numeric IS DISTINCT FROM b.easements::numeric) as easements,
    count(*) FILTER (WHERE a.ownertype IS DISTINCT FROM b.ownertype) as ownertype,
    count(*) FILTER (WHERE a.ownername IS DISTINCT FROM b.ownername) as ownername,
    count(*) FILTER (WHERE abs(a.lotarea::numeric - b.lotarea::numeric)>=5 OR 
        ((a.lotarea IS NULL)::int + (b.lotarea IS NULL)::int) = 1) as lotarea,
    count(*) FILTER (WHERE abs(a.bldgarea::numeric - b.bldgarea::numeric)>=5 OR 
        ((a.bldgarea IS NULL)::int + (b.bldgarea IS NULL)::int) = 1) as bldgarea,
    count(*) FILTER (WHERE abs(a.comarea::numeric - b.comarea::numeric)>=5 OR 
        ((a.comarea IS NULL)::int + (b.comarea IS NULL)::int) = 1) as comarea,
    count(*) FILTER (WHERE abs(a.resarea::numeric - b.resarea::numeric)>=5 OR 
        ((a.resarea IS NULL)::int + (b.resarea IS NULL)::int) = 1) as resarea,
    count(*) FILTER (WHERE abs(a.officearea::numeric - b.officearea::numeric)>=5 OR 
        ((a.officearea IS NULL)::int + (b.officearea IS NULL)::int) = 1) as officearea,
    count(*) FILTER (WHERE abs(a.retailarea::numeric - b.retailarea::numeric)>=5 OR 
        ((a.retailarea IS NULL)::int + (b.retailarea IS NULL)::int) = 1) as retailarea,
    count(*) FILTER (WHERE abs(a.garagearea::numeric - b.garagearea::numeric)>=5 OR 
        ((a.garagearea IS NULL)::int + (b.garagearea IS NULL)::int) = 1) as garagearea,
    count(*) FILTER (WHERE abs(a.strgearea::numeric - b.strgearea::numeric)>=5 OR 
        ((a.strgearea IS NULL)::int + (b.strgearea IS NULL)::int) = 1) as strgearea,
    count(*) FILTER (WHERE abs(a.factryarea::numeric - b.factryarea::numeric)>=5 OR 
        ((a.factryarea IS NULL)::int + (b.factryarea IS NULL)::int) = 1) as factryarea,
    count(*) FILTER (WHERE abs(a.otherarea::numeric - b.otherarea::numeric)>=5 OR 
        ((a.otherarea IS NULL)::int + (b.otherarea IS NULL)::int) = 1) as otherarea,
    count(*) FILTER (WHERE a.areasource IS DISTINCT FROM b.areasource) as areasource,
    count(*) FILTER (WHERE a.numbldgs::numeric IS DISTINCT FROM b.numbldgs::numeric) 
    as numbldgs,
    count(*) FILTER (WHERE a.numfloors::numeric IS DISTINCT FROM b.numfloors::numeric)
    as numfloors,
    count(*) FILTER (WHERE a.unitsres::numeric IS DISTINCT FROM b.unitsres::numeric) 
    as unitsres,
    count(*) FILTER (WHERE a.unitstotal::numeric IS DISTINCT FROM b.unitstotal::numeric) 
    as unitstotal,
    count(*) FILTER (WHERE abs(a.lotfront::numeric - b.lotfront::numeric)>=5 OR 
        ((a.lotfront IS NULL)::int + (b.lotfront IS NULL)::int) = 1) as lotfront,
    count(*) FILTER (WHERE abs(a.lotdepth::numeric - b.lotdepth::numeric)>=5 OR 
        ((a.lotdepth IS NULL)::int + (b.lotdepth IS NULL)::int) = 1) as lotdepth,
    count(*) FILTER (WHERE abs(a.bldgfront::numeric - b.bldgfront::numeric)>=5 OR 
        ((a.bldgfront IS NULL)::int + (b.bldgfront IS NULL)::int) = 1) as bldgfront,
    count(*) FILTER (WHERE abs(a.bldgdepth::numeric - b.bldgdepth::numeric)>=5 OR 
        ((a.bldgdepth IS NULL)::int + (b.bldgdepth IS NULL)::int) = 1) as bldgdepth,
    count(*) FILTER (WHERE a.ext IS DISTINCT FROM b.ext) as ext,
    count(*) FILTER (WHERE a.proxcode IS DISTINCT FROM b.proxcode) as proxcode,
    count(*) FILTER (WHERE a.irrlotcode IS DISTINCT FROM b.irrlotcode)  as irrlotcode,
    count(*) FILTER (WHERE a.lottype IS DISTINCT FROM b.lottype) as lottype,
    count(*) FILTER (WHERE a.bsmtcode IS DISTINCT FROM b.bsmtcode)  as bsmtcode,
    count(*) FILTER (WHERE abs(a.assessland::numeric - b.assessland::numeric)>=10 OR 
        ((a.assessland IS NULL)::int + (b.assessland IS NULL)::int) = 1) as assessland,
    count(*) FILTER (WHERE abs(a.assesstot::numeric - b.assesstot::numeric)>=10 OR 
        ((a.assesstot IS NULL)::int + (b.assesstot IS NULL)::int) = 1) as assesstot,
    count(*) FILTER (WHERE abs(a.exempttot::numeric - b.exempttot::numeric)>=10 OR 
        ((a.exempttot IS NULL)::int + (b.exempttot IS NULL)::int) = 1) as exempttot,
    count(*) FILTER (WHERE a.yearbuilt::numeric IS DISTINCT FROM b.yearbuilt::numeric)  
    as yearbuilt,
    count(*) FILTER (WHERE a.yearalter1::numeric IS DISTINCT FROM b.yearalter1::numeric) 
    as yearalter1,
    count(*) FILTER (WHERE a.yearalter2::numeric IS DISTINCT FROM b.yearalter2::numeric) 
    as yearalter2,
    count(*) FILTER (WHERE a.histdist IS DISTINCT FROM b.histdist) as histdist,
    count(*) FILTER (WHERE a.landmark IS DISTINCT FROM b.landmark) as landmark,
    count(*) FILTER (WHERE a.builtfar::double precision IS DISTINCT FROM 
    b.builtfar::double precision) as builtfar,
    count(*) FILTER (WHERE a.residfar::double precision IS DISTINCT FROM 
    b.residfar::double precision) as residfar,
    count(*) FILTER (WHERE a.commfar::double precision IS DISTINCT FROM 
    b.commfar::double precision) as commfar,
    count(*) FILTER (WHERE a.facilfar::double precision IS DISTINCT FROM 
    b.facilfar::double precision) as facilfar,
    count(*) FILTER (WHERE a.borocode::numeric IS DISTINCT FROM  b.borocode::numeric) 
    as borocode,
    count(*) FILTER (WHERE a.condono::numeric IS DISTINCT FROM b.condono::numeric) as condono,
    count(*) FILTER (WHERE a.tract2010 IS DISTINCT FROM b.tract2010) as tract2010,
    count(*) FILTER (WHERE abs(a.xcoord::numeric - b.xcoord::numeric)>=1 OR 
        ((a.xcoord IS NULL)::int + (b.xcoord IS NULL)::int) = 1) as xcoord,
    count(*) FILTER (WHERE abs(a.ycoord::numeric - b.ycoord::numeric)>=1 OR 
        ((a.ycoord IS NULL)::int + (b.ycoord IS NULL)::int) = 1) as ycoord,
    count(*) FILTER (WHERE abs(a.latitude::numeric - b.latitude::numeric)>=.0001 OR 
        ((a.latitude IS NULL)::int + (b.latitude IS NULL)::int) = 1) as latitude,
    count(*) FILTER (WHERE abs(a.longitude::numeric - b.longitude::numeric)>=.0001 OR 
        ((a.longitude IS NULL)::int + (b.longitude IS NULL)::int) = 1) as longitude,
    count(*) FILTER (WHERE a.zonemap IS DISTINCT FROM b.zonemap) as zonemap,
    count(*) FILTER (WHERE a.zmcode IS DISTINCT FROM b.zmcode) as zmcode,
    count(*) FILTER (WHERE a.sanborn IS DISTINCT FROM b.sanborn) as sanborn,
    count(*) FILTER (WHERE a.taxmap IS DISTINCT FROM b.taxmap) as taxmap,
    count(*) FILTER (WHERE a.edesignum IS DISTINCT FROM b.edesignum) as edesignum,
    count(*) FILTER (WHERE a.appbbl::double precision 
        IS DISTINCT FROM b.appbbl::double precision) as appbbl,
    count(*) FILTER (WHERE a.appdate IS DISTINCT FROM b.appdate) as appdate,
    count(*) FILTER (WHERE a.plutomapid IS DISTINCT FROM b.plutomapid)  as plutomapid,
    count(*) FILTER (WHERE a.sanitdistrict IS DISTINCT FROM b.sanitdistrict) as sanitdistrict,
    count(*) FILTER (WHERE a.healthcenterdistrict::numeric 
        IS DISTINCT FROM b.healthcenterdistrict::numeric) as healthcenterdistrict,
    count(*) FILTER (WHERE a.firm07_flag IS DISTINCT FROM b.firm07_flag) as firm07_flag,
    count(*) FILTER (WHERE a.pfirm15_flag IS DISTINCT FROM b.pfirm15_flag)  as pfirm15_flag
    FROM archive_pluto a
INNER JOIN previous_pluto b
ON (a.bbl::float::bigint = b.bbl::float::bigint)
:CONDITION)
