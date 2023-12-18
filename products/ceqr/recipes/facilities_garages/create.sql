/*
DESCRIPTION:
    this dataset is taking all maintenance and garages from facilities database
    there's no build.sql or build.py because the source table is in EDM_DATA, 
    and output will be stored in EDM_DATA

INPUT: 
    facilities (
        facname     text,
        addressnum  text,
        streetname  text,
        address     text,
        city        text,
        zipcode     text,
        boro        text,
        borocode    text,
        bin         text,
        bbl         text,
        commboard   text,
        nta         text,
        council     text,
        schooldist  text,
        policeprct  text,
        censtract   text,
        factype     text,
        facsubgrp   text,
        facgroup    text,
        facdomain   text,
        servarea    text,
        opname      text,
        opabbrev    text,
        optype      text,
        overagency  text,
        overabbrev  text,
        overlevel   text,
        capacity    text,
        captype     text,
        proptype    text,
        latitude    text,
        longitude   text,
        xcoord      text,
        ycoord      text,
        datasource  text,
        uid         text,
        geom        geometry(Geometry,4326)
    )

OUTPUT: 
    facilities_garages (
        same schema as facilities
    )
*/
CREATE TEMP TABLE facilities_garages as (
    SELECT
        * 
    FROM facilities.latest
    WHERE facdomain ~* 'Core Infrastructure and Transportation'
    OR (facdomain ~* 'Administration of Government'
    AND facsubgrp ~* 'Maintenance and Garages')
);

DROP TABLE IF EXISTS :NAME.:"VERSION" CASCADE;
SELECT *
INTO :NAME.:"VERSION"
FROM facilities_garages;

DROP VIEW IF EXISTS :NAME.latest;
CREATE VIEW :NAME.latest AS (
    SELECT :'VERSION' as v, * 
    FROM :NAME.:"VERSION"
);