-- create table with target field names
DROP TABLE IF EXISTS dof_pts_propmaster;
CREATE TABLE dof_pts_propmaster AS (
    SELECT
        boro,
        block AS tb,
        lot AS tl,
        parid AS bbl,
        street_name,
        housenum_lo,
        housenum_hi,
        aptno,
        zip_code AS zip,
        bldg_class AS bldgcl,
        ease,
        av_owner AS owner,
        replace(land_area, '+', '')::double precision AS land_area,
        replace(gross_sqft, '+', '')::double precision AS gross_sqft,
        replace(residential_area_gross, '+', '')::double precision AS residarea,
        replace(office_area_gross, '+', '')::double precision AS officearea,
        replace(retail_area_gross, '+', '')::double precision AS retailarea,
        replace(garage_area, '+', '')::double precision AS garagearea,
        replace(storage_area_gross, '+', '')::double precision AS storagearea,
        replace(factory_area_gross, '+', '')::double precision AS factoryarea,
        replace(other_area_gross, '+', '')::double precision AS otherarea,
        replace(num_bldgs, '+', '')::double precision AS bldgs,
        replace(bld_story, '+', '')::double precision AS story,
        replace(coop_apts, '+', '')::double precision AS coop_apts,
        replace(units, '+', '')::double precision AS units,
        bld_ext AS ext,
        lot_irreg AS irreg,
        replace(curactland, '+', '')::double precision AS curavl_act,
        replace(curacttot, '+', '')::double precision AS curavt_act,
        replace(curactextot, '+', '')::double precision AS curext_act,
        yrbuilt,
        -- current values contain the most up to date public values.  
        -- June to January current values have the Final value from the prior year.
        -- January to May current values contain the Tentative values.
        -- After May current values contain the Final values.
        -- After May 25th (the date the final roll is released)  it will contain the final values
        yralt1,
        -- pyactland
        yralt2,
        -- pyacttot
        condo_number,
        -- pyactextot
        appt_boro AS ap_boro,
        appt_block AS ap_block,
        appt_lot AS ap_lot,
        appt_ease AS ap_ease,
        appt_date AS ap_date,
        round(replace(lot_frt, '+', '')::numeric, 2) AS lfft,
        round(replace(lot_dep, '+', '')::numeric, 2) AS ldft,
        round(replace(bld_frt, '+', '')::numeric, 2) AS bfft,
        round(replace(bld_dep, '+', '')::numeric, 2) AS bdft
    FROM pluto_pts
);
