-- create table with target field names
DROP TABLE IF EXISTS dof_pts_propmaster;
CREATE TABLE dof_pts_propmaster AS (
    SELECT 
        boro,
	    block as tb,
	    lot as tl,
        parid as BBL,
	    street_name,
	    housenum_lo,
	    housenum_hi,
	    aptno,
	    zip_code as zip,
	    bldg_class as BLDGCL,
	    ease,
	    av_owner as owner,
        REPLACE(land_area, '+', '')::double precision as LAND_AREA,
        REPLACE(gross_sqft, '+', '')::double precision as GROSS_SQFT,
        REPLACE(residential_area_gross, '+', '')::double precision as RESIDAREA,
        REPLACE(office_area_gross, '+', '')::double precision as OFFICEAREA,
        REPLACE(retail_area_gross, '+', '')::double precision as RETAILAREA,
        REPLACE(garage_area, '+', '')::double precision as GARAGEAREA,
        REPLACE(storage_area_gross, '+', '')::double precision as STORAGEAREA,
        REPLACE(factory_area_gross, '+', '')::double precision as FACTORYAREA,
        REPLACE(other_area_gross, '+', '')::double precision as OTHERAREA,
        REPLACE(num_bldgs, '+', '')::double precision as BLDGS,
        REPLACE(bld_story, '+', '')::double precision as STORY,
        REPLACE(coop_apts, '+', '')::double precision as COOP_APTS,
        REPLACE(units, '+', '')::double precision as UNITS,
        round(REPLACE(lot_frt, '+', '')::numeric, 2) as LFFT,
        round(REPLACE(lot_dep, '+', '')::numeric, 2) as LDFT,
        round(REPLACE(bld_frt, '+', '')::numeric, 2) as BFFT,
        round(REPLACE(bld_dep, '+', '')::numeric, 2) as BDFT,
        bld_ext as EXT,
        lot_irreg as IRREG,
        -- current values contain the most up to date public values.  
        -- June to January current values have the Final value from the prior year.
        -- January to May current values contain the Tentative values.
        -- After May current values contain the Final values.
        -- After May 25th (the date the final roll is released)  it will contain the final values
        REPLACE(curactland, '+', '')::double precision as CURAVL_ACT,
        -- pyactland
        REPLACE(curacttot, '+', '')::double precision as CURAVT_ACT,
        -- pyacttot
        REPLACE(curactextot, '+', '')::double precision as CUREXT_ACT,
        -- pyactextot
	    yrbuilt,
	    yralt1,
	    yralt2,
	    condo_number,
	    appt_boro as AP_BORO,
        appt_block as AP_BLOCK,
	    appt_lot as AP_LOT,
	    appt_ease as AP_EASE,
	    appt_date as AP_DATE
    FROM pluto_pts
);
