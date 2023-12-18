/*
DESCRIPTION:
    We are taking all fields directly from nysdot_aadt.latest in RECIPES_ENGINE
    nysdot_aadt is pulled from NYSDOT arcgis server using the following url: 
    https://gis3.dot.ny.gov/arcgis/rest/services/TDV/MapServer/4/query?where=MUNI%3D%27CITY+OF+NEW+YORK%27&outFields=*&f=json

INPUT:
    nysdot_aadt.latest (
        - you can see fields included below in the select statement
        - Check create.sql for more detailed schema
        - Note that wkb_geometry -> geometry(MultiLineString,4326)
    )
    
OUTPUT: 
    TEMP tmp in exact same schema 
    as nysdot_aadt.latest
*/
CREATE TEMP TABLE tmp as (
    SELECT 
        enddesc,
        muni,
        tdv_route,
        aadt_year,
        data_type,
        vol_txt,
        class_txt,
        speed_txt,
        vol_tdv,
        class_tdv,
        speed_tdv,
        "shape.stlength()",
        rc_id,
        loc_error,
        objectid,
        aadt,
        objectid_1,
        ccst,
        begdesc,
        fc,
        perc_truck,
        count_type,
        su_aadt,
        cu_aadt,
        countyr,
        aadt_last_,
        gis_id,
        firstofbeg,
        lastofend_,
        wkb_geometry as geom
    FROM nysdot_aadt.latest
);

\COPY tmp TO PSTDOUT DELIMITER ',' CSV HEADER;